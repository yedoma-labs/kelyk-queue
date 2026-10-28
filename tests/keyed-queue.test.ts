import { afterEach, describe, expect, it, vi } from "vitest";
import { createKeyedQueue, KeyedQueueError } from "../src/index";

function deferred<T = void>() {
	let resolve!: (value: T) => void;
	let reject!: (reason: unknown) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
}

/** Lets queued microtasks (task starts, promise chains) run. */
async function flush(): Promise<void> {
	for (let i = 0; i < 10; i++) {
		await Promise.resolve();
	}
}

function waitForAbort(signal: AbortSignal): Promise<never> {
	return new Promise<never>((_, reject) => {
		signal.addEventListener("abort", () => reject(signal.reason), { once: true });
	});
}

function expectQueueError(promise: Promise<unknown>, code: string): Promise<void> {
	return promise.then(
		() => {
			throw new Error(`expected rejection with ${code}`);
		},
		(error: unknown) => {
			expect(error).toBeInstanceOf(KeyedQueueError);
			expect((error as KeyedQueueError).code).toBe(code);
		},
	);
}

afterEach(() => {
	vi.useRealTimers();
});

describe("ordering", () => {
	it("runs tasks of the same key in FIFO order and never overlaps them", async () => {
		const queue = createKeyedQueue();
		const order: number[] = [];
		let running = 0;
		let maxRunning = 0;

		const tasks = [30, 1, 15].map((delay, index) =>
			queue.run("order-1", async () => {
				running++;
				maxRunning = Math.max(maxRunning, running);
				await new Promise((resolve) => setTimeout(resolve, delay));
				order.push(index);
				running--;
				return index;
			}),
		);

		await expect(Promise.all(tasks)).resolves.toEqual([0, 1, 2]);
		expect(order).toEqual([0, 1, 2]);
		expect(maxRunning).toBe(1);
	});

	it("runs tasks of different keys in parallel", async () => {
		const queue = createKeyedQueue();
		const gates = [deferred(), deferred()];
		let started = 0;
		let bothStarted!: () => void;
		const bothStartedPromise = new Promise<void>((resolve) => {
			bothStarted = resolve;
		});

		const tasks = ["a", "b"].map((key, index) =>
			queue.run(key, async () => {
				started++;
				if (started === 2) {
					bothStarted();
				}
				await gates[index]?.promise;
				return key;
			}),
		);

		await bothStartedPromise;
		expect(started).toBe(2);
		expect(queue.size().active).toBe(2);

		gates[0]?.resolve();
		gates[1]?.resolve();
		await expect(Promise.all(tasks)).resolves.toEqual(["a", "b"]);
	});

	it("never exceeds the global concurrency limit", async () => {
		const queue = createKeyedQueue({ concurrency: 3 });
		let running = 0;
		let maxRunning = 0;

		const tasks = Array.from({ length: 20 }, (_, index) =>
			queue.run(`key-${index}`, async () => {
				running++;
				maxRunning = Math.max(maxRunning, running);
				await new Promise((resolve) => setTimeout(resolve, 5));
				running--;
			}),
		);

		await Promise.all(tasks);
		expect(maxRunning).toBe(3);
	});
});

describe("errors", () => {
	it("rejects the failing run() with the task error and does not block later tasks of the key", async () => {
		const queue = createKeyedQueue();
		const boom = new Error("boom");

		const failing = queue.run("order-2", async () => {
			throw boom;
		});
		const following = queue.run("order-2", async () => "ok");

		await expect(failing).rejects.toBe(boom);
		await expect(following).resolves.toBe("ok");
	});

	it("passes synchronous throws of the task to run()", async () => {
		const queue = createKeyedQueue();
		const error = new Error("sync");

		await expect(
			queue.run("sync", () => {
				throw error;
			}),
		).rejects.toBe(error);
	});
});

describe("timeout and abort", () => {
	it("aborts the task signal on timeout and rejects with TIMEOUT", async () => {
		vi.useFakeTimers();
		const queue = createKeyedQueue({ taskTimeoutMs: 50 });
		let seenSignal: AbortSignal | undefined;

		const promise = queue.run("slow", (signal) => {
			seenSignal = signal;
			return waitForAbort(signal);
		});
		const assertion = expectQueueError(promise, "TIMEOUT");

		await flush();
		await vi.advanceTimersByTimeAsync(50);

		await assertion;
		expect(seenSignal?.aborted).toBe(true);
		expect(seenSignal?.reason).toBeInstanceOf(KeyedQueueError);
	});

	it("keeps the key locked while a task ignores its signal, then continues", async () => {
		const queue = createKeyedQueue();
		const release = deferred();
		const events: string[] = [];

		const stubborn = queue.run(
			"stubborn",
			(signal) => {
				events.push("stubborn-start");
				return new Promise<void>((resolve) => {
					signal.addEventListener("abort", () => {
						// ignores the abort and finishes later
						void release.promise.then(() => {
							events.push("stubborn-end");
							resolve();
						});
					});
				});
			},
			{ timeoutMs: 10 },
		);
		const next = queue.run("stubborn", async () => {
			events.push("next");
		});

		await expectQueueError(stubborn, "TIMEOUT");
		await flush();
		expect(events).toEqual(["stubborn-start"]);

		release.resolve();
		await next;
		expect(events).toEqual(["stubborn-start", "stubborn-end", "next"]);
	});

	it("removes a queued task when the caller aborts before it starts", async () => {
		const queue = createKeyedQueue({ concurrency: 1 });
		const gate = deferred();
		const controller = new AbortController();
		let queuedTaskCalled = false;

		const blocker = queue.run("abort-1", () => gate.promise);
		const queued = queue.run(
			"abort-1",
			async () => {
				queuedTaskCalled = true;
			},
			{ signal: controller.signal },
		);
		expect(queue.size().pending).toBe(1);

		controller.abort();
		await expectQueueError(queued, "ABORTED");
		expect(queue.size().pending).toBe(0);

		gate.resolve();
		await blocker;
		await queue.drain();
		expect(queuedTaskCalled).toBe(false);
	});

	it("aborts the signal of a running task when the caller aborts", async () => {
		const queue = createKeyedQueue();
		const controller = new AbortController();
		let seenSignal: AbortSignal | undefined;

		const running = queue.run(
			"abort-2",
			(signal) => {
				seenSignal = signal;
				return waitForAbort(signal);
			},
			{ signal: controller.signal },
		);

		await flush();
		controller.abort();

		await expectQueueError(running, "ABORTED");
		expect(seenSignal?.aborted).toBe(true);
		expect((seenSignal?.reason as KeyedQueueError).code).toBe("ABORTED");
	});

	it("rejects immediately when the signal is already aborted and never calls the task", async () => {
		const queue = createKeyedQueue();
		const controller = new AbortController();
		controller.abort();
		let called = false;

		await expectQueueError(
			queue.run(
				"abort-3",
				async () => {
					called = true;
				},
				{ signal: controller.signal },
			),
			"ABORTED",
		);
		await queue.drain();
		expect(called).toBe(false);
	});
});

describe("limits", () => {
	it("rejects with QUEUE_FULL when maxPending is exceeded", async () => {
		const queue = createKeyedQueue({ concurrency: 1, maxPending: 2 });
		const gate = deferred();

		const running = queue.run("full", () => gate.promise);
		const second = queue.run("full", async () => 2);
		const third = queue.run("full", async () => 3);
		expect(queue.size()).toEqual({ keys: 1, pending: 2, active: 1 });

		await expectQueueError(
			queue.run("full", async () => 4),
			"QUEUE_FULL",
		);

		gate.resolve();
		await expect(Promise.all([running, second, third])).resolves.toEqual([undefined, 2, 3]);
	});

	it("rejects invalid keys with INVALID_KEY", async () => {
		const queue = createKeyedQueue();
		const task = async () => 1;

		await expectQueueError(queue.run("", task), "INVALID_KEY");
		await expectQueueError(queue.run("x".repeat(257), task), "INVALID_KEY");
		await expectQueueError(queue.run({} as unknown as string, task), "INVALID_KEY");
		await expectQueueError(queue.run(42 as unknown as string, task), "INVALID_KEY");

		await expect(queue.run("x".repeat(256), task)).resolves.toBe(1);
	});

	it("treats prototype-like keys as ordinary keys", async () => {
		const queue = createKeyedQueue();
		await expect(queue.run("__proto__", async () => "a")).resolves.toBe("a");
		await expect(queue.run("constructor", async () => "b")).resolves.toBe("b");
		expect(queue.size().keys).toBe(0);
	});

	it("validates options", () => {
		expect(() => createKeyedQueue({ concurrency: 0 })).toThrow(RangeError);
		expect(() => createKeyedQueue({ concurrency: 1.5 })).toThrow(RangeError);
		expect(() => createKeyedQueue({ maxPending: -1 })).toThrow(RangeError);
		expect(() => createKeyedQueue({ taskTimeoutMs: 0 })).toThrow(RangeError);
		expect(() => createKeyedQueue({ concurrency: Number.POSITIVE_INFINITY })).not.toThrow();
	});
});

describe("drain and close", () => {
	it("drain() resolves after all queued and running tasks have finished", async () => {
		const queue = createKeyedQueue({ concurrency: 2 });
		const done: number[] = [];

		for (let i = 0; i < 6; i++) {
			void queue.run(`drain-${i % 2}`, async () => {
				await new Promise((resolve) => setTimeout(resolve, 2));
				done.push(i);
			});
		}

		await queue.drain();
		expect(done).toHaveLength(6);
		expect(queue.size()).toEqual({ keys: 0, pending: 0, active: 0 });
	});

	it("drain() resolves immediately when the queue is idle", async () => {
		const queue = createKeyedQueue();
		await expect(queue.drain()).resolves.toBeUndefined();
	});

	it("close() rejects new runs with CLOSED", async () => {
		const queue = createKeyedQueue();
		await queue.close();
		await expectQueueError(
			queue.run("late", async () => 1),
			"CLOSED",
		);
	});

	it("close() rejects queued tasks with CLOSED and waits for running ones", async () => {
		const queue = createKeyedQueue({ concurrency: 1 });
		const gate = deferred();
		let queuedCalled = false;
		let runningFinished = false;

		const running = queue.run("close", async () => {
			await gate.promise;
			runningFinished = true;
			return "done";
		});
		const queued = queue.run("close", async () => {
			queuedCalled = true;
		});

		const closing = queue.close();
		await expectQueueError(queued, "CLOSED");

		let closed = false;
		void closing.then(() => {
			closed = true;
		});
		await flush();
		expect(closed).toBe(false);

		gate.resolve();
		await closing;
		expect(runningFinished).toBe(true);
		await expect(running).resolves.toBe("done");
		expect(queuedCalled).toBe(false);
	});

	it("close() on an idle queue resolves immediately", async () => {
		const queue = createKeyedQueue();
		await expect(queue.close()).resolves.toBeUndefined();
	});
});

describe("memory", () => {
	it("removes key entries once their queue is empty and nothing is running", async () => {
		const queue = createKeyedQueue();

		await Promise.all(
			Array.from({ length: 1000 }, (_, index) => queue.run(`short-${index}`, async () => index)),
		);

		expect(queue.size()).toEqual({ keys: 0, pending: 0, active: 0 });
	});

	it("removes the key entry after a queued task was aborted", async () => {
		const queue = createKeyedQueue({ concurrency: 1 });
		const gate = deferred();
		const controller = new AbortController();

		const blocker = queue.run("mem-abort", () => gate.promise);
		const queued = queue.run("mem-abort", async () => 1, { signal: controller.signal });
		controller.abort();
		await expectQueueError(queued, "ABORTED");
		expect(queue.size().keys).toBe(1);

		gate.resolve();
		await blocker;
		expect(queue.size()).toEqual({ keys: 0, pending: 0, active: 0 });
	});
});

describe("stress", () => {
	it("keeps order and non-overlap per key with 10000 tasks on 50 keys", async () => {
		const queue = createKeyedQueue({ concurrency: 8 });
		const keyCount = 50;
		const taskCount = 10_000;
		const seenPerKey = new Map<string, number[]>();
		const runningPerKey = new Map<string, number>();
		let overlapDetected = false;
		let globalRunning = 0;
		let maxGlobalRunning = 0;

		const promises: Promise<void>[] = [];
		for (let i = 0; i < taskCount; i++) {
			const key = `stress-${i % keyCount}`;
			promises.push(
				queue.run(key, async () => {
					const current = (runningPerKey.get(key) ?? 0) + 1;
					runningPerKey.set(key, current);
					if (current > 1) {
						overlapDetected = true;
					}
					globalRunning++;
					maxGlobalRunning = Math.max(maxGlobalRunning, globalRunning);

					const steps = (i * 7) % 4;
					for (let step = 0; step < steps; step++) {
						await new Promise<void>((resolve) => queueMicrotask(resolve));
					}

					const seen = seenPerKey.get(key) ?? [];
					seen.push(i);
					seenPerKey.set(key, seen);
					runningPerKey.set(key, (runningPerKey.get(key) ?? 1) - 1);
					globalRunning--;
				}),
			);
		}

		await Promise.all(promises);

		expect(overlapDetected).toBe(false);
		expect(maxGlobalRunning).toBeLessThanOrEqual(8);
		expect(seenPerKey.size).toBe(keyCount);
		for (let k = 0; k < keyCount; k++) {
			const expected = Array.from({ length: taskCount / keyCount }, (_, n) => k + n * keyCount);
			expect(seenPerKey.get(`stress-${k}`)).toEqual(expected);
		}
		expect(queue.size()).toEqual({ keys: 0, pending: 0, active: 0 });
	});
});
