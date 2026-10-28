import { KeyedQueueError } from "./errors";

export interface KeyedQueueOptions {
	/** Global upper bound of tasks running at the same time, across all keys. Default: Infinity. */
	concurrency?: number;
	/** Default timeout per task. The clock starts when the task starts, not when it is queued. */
	taskTimeoutMs?: number;
	/** Maximum number of tasks waiting to start (all keys together). Default: 10000. */
	maxPending?: number;
}

export interface RunOptions {
	/** Overrides `taskTimeoutMs` for this task. */
	timeoutMs?: number;
	/** Aborts the task: removed if still queued, otherwise its signal is aborted. */
	signal?: AbortSignal;
}

export interface KeyedQueueSize {
	/** Keys that currently have queued or running tasks. */
	keys: number;
	/** Tasks waiting to start. */
	pending: number;
	/** Tasks currently running. */
	active: number;
}

export interface KeyedQueue {
	/**
	 * Queues a task under `key`. Tasks with the same key run strictly in FIFO order and never
	 * overlap. Tasks with different keys run in parallel, limited by `concurrency`.
	 */
	run<T>(key: string, task: (signal: AbortSignal) => Promise<T>, options?: RunOptions): Promise<T>;
	size(): KeyedQueueSize;
	/** Resolves once no task is queued or running. */
	drain(): Promise<void>;
	/**
	 * Rejects new `run()` calls and tasks that have not started yet (code `CLOSED`), then waits
	 * for running tasks to settle.
	 */
	close(): Promise<void>;
}

const DEFAULT_MAX_PENDING = 10_000;
const MAX_KEY_LENGTH = 256;
/** Largest delay supported by setTimeout. Larger values would fire immediately. */
const MAX_TIMER_MS = 2_147_483_647;

type Outcome = { ok: true; value: unknown } | { ok: false; error: unknown };

interface Job {
	readonly key: string;
	readonly task: (signal: AbortSignal) => Promise<unknown>;
	readonly timeoutMs: number | undefined;
	readonly callerSignal: AbortSignal | undefined;
	readonly controller: AbortController;
	readonly resolve: (value: unknown) => void;
	readonly reject: (reason: unknown) => void;
	onCallerAbort: (() => void) | undefined;
	timer: ReturnType<typeof setTimeout> | undefined;
	/** True once the job holds a concurrency slot. */
	started: boolean;
	/** True once the promise returned by run() has been settled. */
	settled: boolean;
}

interface KeyState {
	jobs: Job[];
	running: boolean;
}

/**
 * Creates an in-process queue that serializes tasks per key and bounds the total number of
 * concurrently running tasks. Keys are removed from memory as soon as they are idle.
 *
 * @throws {RangeError} if an option is invalid.
 */
export function createKeyedQueue(options: KeyedQueueOptions = {}): KeyedQueue {
	const concurrency = validateLimit("concurrency", options.concurrency ?? Number.POSITIVE_INFINITY);
	const maxPending = validateLimit("maxPending", options.maxPending ?? DEFAULT_MAX_PENDING);
	const defaultTimeoutMs = validateTimeout("taskTimeoutMs", options.taskTimeoutMs);

	const keys = new Map<string, KeyState>();
	/** Keys that have waiting jobs and are not running, in FIFO order. Each key appears at most once. */
	const ready = new Set<string>();
	let pending = 0;
	let active = 0;
	let closed = false;
	let idleWaiters: Array<() => void> = [];

	function run<T>(
		key: string,
		task: (signal: AbortSignal) => Promise<T>,
		runOptions: RunOptions = {},
	): Promise<T> {
		if (!isValidKey(key)) {
			return Promise.reject(
				new KeyedQueueError(
					"INVALID_KEY",
					`Key must be a non-empty string of at most ${MAX_KEY_LENGTH} characters`,
				),
			);
		}
		if (closed) {
			return Promise.reject(new KeyedQueueError("CLOSED", "Queue is closed"));
		}
		const callerSignal = runOptions.signal;
		if (callerSignal?.aborted) {
			return Promise.reject(
				new KeyedQueueError("ABORTED", "Task was aborted before it was queued"),
			);
		}
		if (pending >= maxPending) {
			return Promise.reject(
				new KeyedQueueError("QUEUE_FULL", `Queue is full (maxPending: ${maxPending})`),
			);
		}

		let timeoutMs: number | undefined;
		try {
			timeoutMs =
				runOptions.timeoutMs === undefined
					? defaultTimeoutMs
					: validateTimeout("timeoutMs", runOptions.timeoutMs);
		} catch (error) {
			return Promise.reject(error);
		}

		return new Promise<T>((resolvePromise, rejectPromise) => {
			const job: Job = {
				key,
				task,
				timeoutMs,
				callerSignal,
				controller: new AbortController(),
				resolve: (value) => resolvePromise(value as T),
				reject: rejectPromise,
				onCallerAbort: undefined,
				timer: undefined,
				started: false,
				settled: false,
			};
			if (callerSignal) {
				job.onCallerAbort = () => abortByCaller(job);
				callerSignal.addEventListener("abort", job.onCallerAbort, { once: true });
			}
			enqueue(job);
		});
	}

	function enqueue(job: Job): void {
		let state = keys.get(job.key);
		if (!state) {
			state = { jobs: [], running: false };
			keys.set(job.key, state);
		}
		if (!state.running) {
			ready.add(job.key);
		}
		state.jobs.push(job);
		pending++;
		schedule();
	}

	/** Starts waiting jobs of ready keys while there are free concurrency slots. */
	function schedule(): void {
		while (active < concurrency) {
			const next = ready.values().next();
			if (next.done) {
				return;
			}
			const key = next.value;
			ready.delete(key);
			const state = keys.get(key);
			if (!state || state.running) {
				continue;
			}
			const job = state.jobs.shift();
			if (!job) {
				keys.delete(key);
				continue;
			}
			state.running = true;
			job.started = true;
			pending--;
			active++;
			startJob(job);
		}
	}

	/** Invokes the task in a microtask, so run() never calls user code synchronously. */
	function startJob(job: Job): void {
		queueMicrotask(() => {
			if (!job.settled && job.timeoutMs !== undefined) {
				job.timer = setTimeout(() => onTimeout(job), job.timeoutMs);
			}
			let execution: Promise<unknown>;
			try {
				execution = Promise.resolve(job.task(job.controller.signal));
			} catch (error) {
				execution = Promise.reject(error);
			}
			void execution.then(
				(value) => finish(job, { ok: true, value }),
				(error: unknown) => finish(job, { ok: false, error }),
			);
		});
	}

	/** Called when the task itself has settled. Releases the concurrency slot and key lock. */
	function finish(job: Job, outcome: Outcome): void {
		settle(job, outcome);
		active--;
		const state = keys.get(job.key);
		if (state) {
			state.running = false;
			if (state.jobs.length > 0) {
				ready.add(job.key);
			} else {
				keys.delete(job.key);
			}
		}
		schedule();
		notifyIdle();
	}

	function onTimeout(job: Job): void {
		job.timer = undefined;
		if (job.settled) {
			return;
		}
		const error = new KeyedQueueError("TIMEOUT", `Task timed out after ${job.timeoutMs} ms`);
		job.controller.abort(error);
		settle(job, { ok: false, error });
	}

	function abortByCaller(job: Job): void {
		if (job.settled) {
			return;
		}
		const error = new KeyedQueueError("ABORTED", "Task was aborted by the caller");
		if (job.started) {
			job.controller.abort(error);
			settle(job, { ok: false, error });
			return;
		}
		removeQueued(job);
		settle(job, { ok: false, error });
		notifyIdle();
	}

	function removeQueued(job: Job): void {
		const state = keys.get(job.key);
		if (!state) {
			return;
		}
		const index = state.jobs.indexOf(job);
		if (index !== -1) {
			state.jobs.splice(index, 1);
			pending--;
		}
		if (!state.running && state.jobs.length === 0) {
			keys.delete(job.key);
			ready.delete(job.key);
		}
	}

	/** Settles the run() promise exactly once and detaches all listeners and timers. */
	function settle(job: Job, outcome: Outcome): void {
		if (job.settled) {
			return;
		}
		job.settled = true;
		if (job.timer !== undefined) {
			clearTimeout(job.timer);
			job.timer = undefined;
		}
		if (job.callerSignal && job.onCallerAbort) {
			job.callerSignal.removeEventListener("abort", job.onCallerAbort);
		}
		if (outcome.ok) {
			job.resolve(outcome.value);
		} else {
			job.reject(outcome.error);
		}
	}

	function size(): KeyedQueueSize {
		return { keys: keys.size, pending, active };
	}

	function drain(): Promise<void> {
		return waitForIdle();
	}

	function close(): Promise<void> {
		if (!closed) {
			closed = true;
			for (const [key, state] of keys) {
				const jobs = state.jobs.splice(0);
				pending -= jobs.length;
				for (const job of jobs) {
					settle(job, {
						ok: false,
						error: new KeyedQueueError("CLOSED", "Queue was closed before the task started"),
					});
				}
				if (!state.running) {
					keys.delete(key);
				}
			}
			ready.clear();
			notifyIdle();
		}
		return waitForIdle();
	}

	function waitForIdle(): Promise<void> {
		if (pending === 0 && active === 0) {
			return Promise.resolve();
		}
		return new Promise<void>((resolveIdle) => {
			idleWaiters.push(resolveIdle);
		});
	}

	function notifyIdle(): void {
		if (pending !== 0 || active !== 0 || idleWaiters.length === 0) {
			return;
		}
		const waiters = idleWaiters;
		idleWaiters = [];
		for (const resolveIdle of waiters) {
			resolveIdle();
		}
	}

	return { run, size, drain, close };
}

function isValidKey(key: unknown): key is string {
	return typeof key === "string" && key.length > 0 && key.length <= MAX_KEY_LENGTH;
}

function validateLimit(name: string, value: number): number {
	if (value === Number.POSITIVE_INFINITY) {
		return value;
	}
	if (!Number.isInteger(value) || value < 1) {
		throw new RangeError(`${name} must be a positive integer or Infinity, got ${value}`);
	}
	return value;
}

/** Returns undefined for "no timeout" (undefined or Infinity). */
function validateTimeout(name: string, value: number | undefined): number | undefined {
	if (value === undefined || value === Number.POSITIVE_INFINITY) {
		return undefined;
	}
	if (!(value > 0) || value > MAX_TIMER_MS) {
		throw new RangeError(`${name} must be a number greater than 0 and at most ${MAX_TIMER_MS}`);
	}
	return value;
}
