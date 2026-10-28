# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.x     | :white_check_mark: |

Only the latest published 0.x release receives fixes.

## Security Model

`@yedoma-labs/kelyk-queue` is a **process-local** in-memory queue. It runs caller-supplied async
functions and does not sandbox them. The security model assumes:

- **Task functions** are written by trusted developers. They run with the full privileges of the host process.
- **Keys** are strings that may originate from external input (e.g. an order ID from a request).
  They are validated (non-empty, max. 256 characters) and stored in a `Map`, so keys such as
  `__proto__` or `constructor` are plain map entries and cannot pollute `Object.prototype`.
- **Coordination is in-process only.** The queue provides no distributed locking. Several
  processes or instances each have their own queue.

## Security Features

1. **Prototype safety**: key storage uses `Map`, never a plain object.
2. **Input validation**: keys must be non-empty strings of at most 256 characters, otherwise
   `INVALID_KEY` is thrown.
3. **Bounded memory**:
   - `maxPending` (default 10000) caps the number of queued tasks. Overflow is rejected immediately
     with `QUEUE_FULL`.
   - Per-key entries are removed as soon as their queue is empty and nothing is running, so the
     key map does not grow with the number of distinct keys used over time.
4. **Cooperative cancellation**: timeouts and caller aborts fire an `AbortSignal` that is passed to
   the running task. Queued tasks that have not started yet are removed without being executed.

## Known Limitations

1. **Timeouts cannot forcibly stop a task.** JavaScript cannot interrupt synchronous or
   non-cooperative async code. A task that ignores its `AbortSignal` keeps running in the background
   after `run()` has rejected with `TIMEOUT`. Its slot in the concurrency limit and its key lock are
   released only when the task's promise settles.
   - **Mitigation:** pass `signal` into every `fetch`, database call, or other cancellable operation
     inside the task.
2. **No resource quotas for task work.** The queue limits how many tasks run and wait, not how much
   CPU or memory a single task uses.
3. **No persistence.** Pending tasks are lost when the process exits.

## Reporting Vulnerabilities

Please report security issues privately through GitHub Security Advisories on
https://github.com/yedoma-labs/kelyk-queue (the "Report a vulnerability" button). Do not open a
public issue for security problems.

Please include:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## License

MIT - See [LICENSE](LICENSE) for details.
