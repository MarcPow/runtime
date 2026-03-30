# io_uring Integration — Final Status

## Breakthrough: Simplified Architecture

A 170-line standalone prototype proved the entire event loop is unnecessary:

```
User thread:     Write SQE → io_uring_enter(submit=1) → return IOPending
Kernel:          Reads SQE → does send/recv → writes CQE → signals eventfd
ThreadPool:      poll() wakes on eventfd → reads CQ ring → dispatches TaskCompletionSource
```

### Prototype results (WSL2, kernel 6.6):
- SEND/RECV: works
- Ping-pong 1000 round-trips: 105µs/rt
- Concurrent 200 send+recv: works (with SQ tail lock)
- Zero hangs, zero lost completions, zero EAGAIN

### What the prototype eliminates vs the original PR:
- Dedicated event loop thread
- MPSC prepare queue + drain cycle
- _sqSubmitLock complexity (replaced by simple lock around SQ tail only)
- EAGAIN retry paths (POLL_ADD, readiness fallback, inline re-prepare)
- EnqueueReadinessFallbackEvent → HandleEvents → blocking TryComplete path
- FinishDirectSqeSubmission / AbortDirectSqeSubmission
- Lock heldSqLock parameter threading
- WakeEventLoop / eventfd write for submission wakeup
- SubmitIoUringBatch / SubmitIoUringOperationsNormalized
- TryAcquireManagedSqeWithRetry
- The entire DispatchPendingIoUringOperation retry cascade

### What stays:
- SQ/CQ ring mmap + teardown
- SQE writers (WriteSendLikeSqe, WriteAcceptSqe, etc.)
- Completion slot pool (for user_data → operation mapping)
- Partial class dispatch: `if (_isIoUringActive) return IoUring*Async(...);`
- Blocking sockets for io_uring (kernel FAST_POLL handles EAGAIN)

### Key design: eventfd for CQ notification
The kernel signals an eventfd when CQEs are produced. A ThreadPool worker
(woken by poll/epoll on the eventfd) drains the CQ ring and dispatches
completions. No dedicated thread needed.

### SQ tail serialization
Multiple threads submitting concurrently need a lock around the SQ tail
bump + SQE write. ~10ns cost. The io_uring_enter() call itself is outside
the lock — the kernel handles concurrent enter() calls.

## Branch
`MarcPow/runtime` branch `fix/iouring-direct-submit-perf`

## Prototype
`/tmp/uring-eventfd/Program.cs` — 170 lines, fully working

## Test Infrastructure
- Azure VM `20.12.235.226`: 2-core, Ubuntu 24.04, kernel 6.17
- Azure VM `74.249.210.155`: 8-core (available)
- WSL2 local: kernel 6.6 — works with raw io_uring (C and C#)

## Progress on Rewrite

### Done:
- Deleted 5 files (MpscQueue, ProvidedBufferRing, Diagnostics, TestHookStubs, CompletionDispatch) — 2,448 lines
- Extracted types/constants into dedicated files
- Renamed all files to `SocketAsyncEngine.Linux.IoUring.X.cs` pattern
- Rewrote `SocketAsyncEngine.Linux.cs` (4,789 → ~400 lines)
- Net: +1,203 -7,341 lines so far

### Remaining:
- Rewrite `SocketAsyncContext.Linux.IoUring.cs` (3,762 lines) to use new engine API
  - 33 references to deleted engine methods
  - Cut multishot accept/recv, shadow listeners, complex buffer management
  - Keep: IoUring*Async dispatchers, completion processing, basic operation tracking
  - Target: ~800 lines
- Fix remaining 15 build errors (all in this one file)
- Clean up Slots.cs and Rings.cs references to deleted features
- Test locally and on Azure VM
