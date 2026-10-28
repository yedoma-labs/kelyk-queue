# @yedoma-labs/kelyk-queue

<picture>
  <source media="(max-width: 640px)" srcset="https://raw.githubusercontent.com/yedoma-labs/assets/main/resized/banner-resized-mobile.png">
  <img src="https://raw.githubusercontent.com/yedoma-labs/assets/main/resized/banner-resized.png" alt="Project Header">
</picture>

[![CI](https://github.com/yedoma-labs/kelyk-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/yedoma-labs/kelyk-queue/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@yedoma-labs/kelyk-queue.svg)](https://www.npmjs.com/package/@yedoma-labs/kelyk-queue)
[![TypeScript](https://img.shields.io/badge/TypeScript-7-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A zero-dependency async queue with **strict ordering per key** and **parallelism across keys**,
plus a global concurrency limit, timeouts, `AbortSignal` support and automatic cleanup of idle keys.

---

## The problem

Events that belong together must be processed one after another. For example, all updates to the
same order must not run at the same time, or they overwrite each other's state. Events for
different orders, however, should run in parallel to keep throughput up.

- `async-mutex` gives you one lock. It has no notion of keys, so you would need to manage a lock
  per key yourself, and it is not maintained anymore (last release 2024).
- `p-queue` limits concurrency globally, but it does not guarantee order per key.

`kelyk-queue` covers the gap: FIFO per key, parallel across keys, bounded in total.

## Installation

```sh
pnpm add @yedoma-labs/kelyk-queue
```

Requires Node.js 20 or newer. The package uses only standard Web APIs (`AbortController`,
`AbortSignal`, `setTimeout`, `queueMicrotask`), so it also runs on Bun and Deno.

## Quick Start

Updates to the same order run one after another. Updates to different orders run in parallel, at
most 10 tasks at a time.

```ts
import { createKeyedQueue, KeyedQueueError } from "@yedoma-labs/kelyk-queue";

const orderQueue = createKeyedQueue({
	concurrency: 10, // at most 10 tasks run at the same time, across all orders
	taskTimeoutMs: 5_000, // default timeout per task
	maxPending: 1_000, // reject new work if 1000 tasks are already waiting
});

async function handleOrderUpdate(orderId: string, changes: Partial<Order>) {
	return orderQueue.run(orderId, async (signal) => {
		// Both calls run strictly after the previous update of the same order has finished.
		const order = await loadOrder(orderId, signal);
		await saveOrder({ ...order, ...changes }, signal);
	});
}

try {
	await handleOrderUpdate("order-42", { status: "shipped" });
} catch (error) {
	if (error instanceof KeyedQueueError && error.code === "TIMEOUT") {
		// the update took longer than 5 seconds
	}
	throw error;
}

// On shutdown: stop accepting work and wait for running tasks.
await orderQueue.close();
```

## API

```ts
createKeyedQueue(options?: {
	concurrency?: number; // positive integer or Infinity, default Infinity
	taskTimeoutMs?: number; // default: no timeout
	maxPending?: number; // positive integer or Infinity, default 10000
}): KeyedQueue;

interface KeyedQueue {
	run<T>(
		key: string,
		task: (signal: AbortSignal) => Promise<T>,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	): Promise<T>;
	size(): { keys: number; pending: number; active: number };
	drain(): Promise<void>;
	close(): Promise<void>;
}
```

- `size()` reports keys with queued or running tasks, tasks waiting to start (`pending`), and
  tasks currently running (`active`).
- `drain()` resolves once nothing is queued or running.
- `close()` see below.

Errors from the queue are `KeyedQueueError` instances with a `code` property. Errors thrown by
your task are passed through unchanged.

## Semantics

| Topic | Behavior |
| --- | --- |
| Order per key | Strict FIFO. Tasks with the same key never overlap. |
| Parallelism | Tasks with different keys run in parallel, up to `concurrency` in total. |
| Task error | Rejects only the `run()` of that task. Later tasks of the same key still run. |
| Timeout | The timer starts when the task starts, not when it is queued. On timeout the task's signal is aborted with a `KeyedQueueError` (`TIMEOUT`) and `run()` rejects with `TIMEOUT`. |
| Timeout and running tasks | The queue cannot stop a task. A task that ignores its signal keeps its key lock and concurrency slot until it settles. |
| Caller abort, task queued | The task is removed and never runs. `run()` rejects with `ABORTED`. |
| Caller abort, task running | The task's signal is aborted with `ABORTED`. `run()` rejects with `ABORTED` right away. The slot is released when the task settles. |
| Already aborted signal | `run()` rejects with `ABORTED` immediately. The task is not queued. |
| `maxPending` | Limits the number of tasks waiting to start, across all keys. Overflow rejects immediately with `QUEUE_FULL`. Running tasks do not count. |
| Key rules | Must be a non-empty string of at most 256 characters (`.length`). Otherwise `run()` rejects with `INVALID_KEY`. |
| Memory | A key is removed as soon as its queue is empty and nothing runs for it. The number of keys does not grow with the number of distinct keys used over time. |
| `close()` | New `run()` calls reject with `CLOSED`. Tasks that have not started are rejected with `CLOSED`. `close()` resolves when all running tasks have settled. Do not await `close()` inside a task. |
| Invalid options | `createKeyedQueue` throws a `RangeError`. An invalid `timeoutMs` in `run()` rejects with a `RangeError`. |

Error codes: `INVALID_KEY`, `QUEUE_FULL`, `TIMEOUT`, `ABORTED`, `CLOSED`.

## Limits

This queue is **process-local**. It is an in-memory structure.

- It does not coordinate across processes or machines. If you run several instances, each one has
  its own queue, and ordering is only guaranteed within one process.
- Pending tasks are lost when the process exits.
- There is no distributed lock. If you need ordering across instances, use a distributed lock
  per key, for example a Redis lock, and keep this queue for in-process concurrency control.

---

## Development

```sh
pnpm install
pnpm lint
pnpm typecheck
pnpm test
pnpm build
```

## License

MIT. See [LICENSE](LICENSE).
