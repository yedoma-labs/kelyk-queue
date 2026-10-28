export type KeyedQueueErrorCode = "INVALID_KEY" | "QUEUE_FULL" | "TIMEOUT" | "ABORTED" | "CLOSED";

/**
 * Error thrown (as a rejection) by {@link KeyedQueue.run} for queue-level failures.
 * Errors thrown by the task itself are passed through unchanged.
 */
export class KeyedQueueError extends Error {
	readonly code: KeyedQueueErrorCode;

	constructor(code: KeyedQueueErrorCode, message: string) {
		super(message);
		this.name = "KeyedQueueError";
		this.code = code;
	}
}
