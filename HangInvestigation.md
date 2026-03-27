# io_uring Throughput Hang Investigation

## Status: Heisenbug — timing-dependent race in multi-thread CQE dispatch

**UPDATE**: `SubmitPendingToKernel` was NOT deleted — it exists and is called correctly.
The root cause is a timing-dependent race condition:
- 20/20 runs pass under strace (syscall overhead serializes threads)
- ~50% failure rate without strace on 80 x 4KB concurrent send+recv
- The strace during the hang shows the event loop spinning on `io_uring_enter(submit=0, GETEVENTS)`
  returning ETIME — waiting for a CQE that never arrives
- This means a recv SQE was submitted but the kernel either lost it or the CQE was consumed
  without dispatching the completion

### Key evidence:
- `SubmitPendingToKernel` EXISTS and IS called (earlier theory of deletion was wrong)
- strace from start: 20/20 passes (strace overhead eliminates timing window)
- strace during hang: event loop spins on `io_uring_enter(submit=0, GETEVENTS)` → ETIME
- This means: the recv SQE was submitted, the kernel is processing it, but the CQE
  never arrives OR the CQE arrives but the tracked operation can't be matched

### Suspected race:
Thread B's `io_uring_enter(submit=1)` can consume Thread A's published-but-not-yet-submitted
SQE as a side effect (kernel reads from SQ head to published tail). If the CQE for Thread A's
operation arrives before Thread A calls its own `io_uring_enter`, the event loop processes it.
This is normally fine because tracking happens under lock before publish.

The real question: is there a path where an SQE is published but the corresponding tracked
operation is NOT yet visible to the event loop? The `_sqSubmitLock` ensures tracking happens
before publish within a single thread's lock scope. But `FreeCompletionSlot` (called from
CQE dispatch on the event loop) also takes `_sqSubmitLock` for the free-list push. If the
event loop's `FreeCompletionSlot` + a user thread's `AllocateCompletionSlot` reuse a slot
while a CQE for the OLD incarnation is still being processed, generations could get confused.

### What to investigate next:
1. Add atomic counters: count SQEs submitted vs CQEs received. On hang, check if they match.
2. Check if `FreeCompletionSlot`'s generation bump races with `TryTakeTrackedIoUringOperation`
3. Check if the POLL_ADD readiness path (dispatching via `DispatchPendingIoUringOperation`)
   can consume a tracked operation and then fail to re-submit it

## The Symptom

Concurrent send + recv on the same TCP socket hangs after ~70-80 operations when
using io_uring. Sequential send-then-recv works perfectly. epoll works perfectly.
Ping-pong (alternating send/recv) works perfectly.

## Test Case

```csharp
// 80 x 4KB sends concurrent with receives — HANGS
var recv = Task.Run(async () => {
    long r = 0;
    while (r < totalBytes) { int n = await s.ReceiveAsync(rb); r += n; }
});
for (int i = 0; i < 80; i++) await c.SendAsync(buf);
await recv; // TIMEOUT
```

The hang is nondeterministic (50% repro rate with 80 sends, 100% with 200+).

## Root Cause

**`SubmitPendingToKernel` — the method that calls `io_uring_enter(submit=N)` from the
user thread — was accidentally deleted during the lock-threading refactoring.**

The call chain is:
```
TryDirectSubmitIoUring
  → IoUringPrepareDirect (writes SQE to ring under _sqSubmitLock)
  → TryTrackDirectlySubmittedOperation (tracks operation under lock)
  → FinishDirectSqeSubmission (publishes SQ tail, zeros _ioUringManagedPendingSubmissions, returns pending count)
  → heldSqLock.Exit() (in finally block)
  → SubmitPendingToKernel(pending) ← THIS METHOD WAS DELETED
```

Without `SubmitPendingToKernel`, the SQE is published to the SQ ring (kernel can see
the tail pointer) but `io_uring_enter()` is never called to make the kernel consume it.
The event loop then calls `io_uring_enter(submit=0, GETEVENTS)` because
`_ioUringManagedPendingSubmissions` was zeroed — so it doesn't submit anything either.

The strace confirms this: during the hang, the event loop repeatedly calls
`io_uring_enter(0, 0, 1, GETEVENTS)` which returns `ETIME` every 50ms. The recv SQE
sits in the SQ ring unconsumed forever.

## Evidence

### strace during hang (Azure VM, kernel 6.17):
```
io_uring_enter(0, 0, 1, IORING_ENTER_GETEVENTS|IORING_ENTER_EXT_ARG|IORING_ENTER_REGISTERED_RING, ...) = -1 ETIME
io_uring_enter(0, 0, 1, IORING_ENTER_GETEVENTS|IORING_ENTER_EXT_ARG|IORING_ENTER_REGISTERED_RING, ...) = -1 ETIME
io_uring_enter(0, 0, 1, IORING_ENTER_GETEVENTS|IORING_ENTER_EXT_ARG|IORING_ENTER_REGISTERED_RING, ...) = -1 ETIME
```
All calls have `submit=0`. No SQEs are ever submitted to the kernel.

### Why it's intermittent:
When the sender and receiver happen to run sequentially (all sends complete before first
recv), there's no issue because the event loop submits the SQEs as part of its
`SubmitIoUringBatch` cycle. The hang only occurs when a recv SQE is submitted from a
user thread via `TryDirectSubmitIoUring` while the event loop is in its GETEVENTS wait.

### Why ping-pong works:
Ping-pong alternates send → recv on the same async flow. Each operation completes before
the next starts. The event loop has time to drain CQEs and submit pending SQEs between
operations.

## Fix

Restore `SubmitPendingToKernel` on `SocketAsyncEngine`:

```csharp
internal void SubmitPendingToKernel(uint toSubmit)
{
    if (toSubmit == 0) return;
    uint enterFlags = 0;
    int ringFd = _ringState.RingFd;
    IoUringEnterWithFallback(ref ringFd, toSubmit, 0, ref enterFlags, out _);
}
```

This was the original method from our first implementation. It was accidentally deleted
when the Python refactoring script replaced `EnterSubmitLock`/`ExitSubmitLock` with
`GetSqSubmitLock()`.

## Other Findings Along the Way

### 1. IORING_RECVSEND_POLL_FIRST (set in SQE ioprio)
We added this flag to tell the kernel to use internal FAST_POLL for EAGAIN handling.
This is how all production io_uring users handle EAGAIN — the kernel retries internally,
userspace never sees EAGAIN. This is correct and should be kept.

### 2. POLL_ADD readiness mechanism
We added `TryQueueReadinessPoll` for EAGAIN retry — one-shot POLLIN for reads,
multi-shot POLLOUT for writes. With RECVSEND_POLL_FIRST, this should rarely be needed
(the kernel handles EAGAIN), but it's a safety net. However, the POLL_ADD code has NOT
been tested because the missing `SubmitPendingToKernel` prevented us from ever getting
far enough.

### 3. Unbounded MPSC prepare queue
We removed the capacity limit on the MPSC prepare queue to prevent silent operation
drops. This is correct — the old bounded queue would silently drop operations, causing
permanent hangs.

## Files Involved

- `SocketAsyncEngine.Linux.cs` — main engine, lock management, SQE submission
- `SocketAsyncEngine.IoUringSqeWriters.Linux.cs` — SQE field writers (RECVSEND_POLL_FIRST)
- `SocketAsyncEngine.IoUringCompletionDispatch.Linux.cs` — CQE dispatch, EAGAIN retry routing
- `SocketAsyncEngine.IoUringSlots.Linux.cs` — completion slot alloc/free
- `SocketAsyncContext.IoUring.Linux.cs` — operation staging, TryDirectSubmitIoUring

## Branch

`MarcPow/runtime` branch `fix/iouring-direct-submit-perf`

## Reproduction Test Code

```csharp
// /tmp/diag2/Program.cs on Azure VMs
// Tests concurrent send + recv on same TCP socket at various counts
using System; using System.Net; using System.Net.Sockets;
using System.Threading; using System.Threading.Tasks; using System.Diagnostics;

foreach (int count in new[] { 50, 60, 70, 80, 90, 100, 150, 200 })
{
    var l = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    l.Bind(new IPEndPoint(IPAddress.Loopback, 0)); l.Listen(1);
    var c = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    c.NoDelay = true;
    await c.ConnectAsync((IPEndPoint)l.LocalEndPoint!);
    var s = await l.AcceptAsync(); s.NoDelay = true;
    byte[] buf = new byte[4096], rb = new byte[65536];
    long totalBytes = (long)count * buf.Length;
    int recvCalls = 0;
    var recv = Task.Run(async () => {
        long r = 0;
        while (r < totalBytes) {
            Interlocked.Increment(ref recvCalls);
            int n = await s.ReceiveAsync(rb);
            if (n == 0) break;
            r += n;
        }
        return r;
    });
    var sw = Stopwatch.StartNew();
    for (int i = 0; i < count; i++) await c.SendAsync(buf);
    if (await Task.WhenAny(recv, Task.Delay(3000)) == recv)
        Console.WriteLine($"  {count,5} x 4KB: {sw.ElapsedMilliseconds,5}ms OK (recvCalls={recvCalls})");
    else
        Console.WriteLine($"  {count,5} x 4KB: TIMEOUT (recvCalls={recvCalls})");
    c.Dispose(); s.Dispose(); l.Dispose();
}
```

### Typical failure output (7/10 fail):
```
Run 1:      80 x 4KB: TIMEOUT (recvCalls=67)
Run 2:      80 x 4KB:    15ms OK
Run 3:      80 x 4KB: TIMEOUT (recvCalls=80)
Run 4:      80 x 4KB: TIMEOUT (recvCalls=69)
Run 5:      80 x 4KB: TIMEOUT (recvCalls=72)
Run 6:      80 x 4KB: TIMEOUT (recvCalls=79)
Run 7:      80 x 4KB:    16ms OK
```

Key observation: `recvCalls=80` means all recv calls were MADE but the last never returned.
`recvCalls=67` means the 67th recv returned but the 68th was never started (the 67th
completion dispatch didn't resume the receiver's await).

### Key behavior:
- Sequential send-then-recv: ALWAYS works
- Concurrent send+recv with 4KB recv buffer: ALWAYS works
- Concurrent send+recv with 65KB recv buffer: FAILS at ~70-80 ops (~50% repro rate)
- Under strace: ALWAYS works (0/20 fail) — Heisenbug
- 4KB recv buffer works because each recv returns exactly one chunk (no partial reads)
- 65KB recv buffer fails because recv returns variable amounts, creating EAGAIN paths

## Experiments Tried

| Change | Result |
|--------|--------|
| Widened `_sqSubmitLock` in `FreeCompletionSlot` (generation bump + state reset under lock) | 20/20 then 3/10 — inconsistent, not the root cause |
| `IORING_RECVSEND_POLL_FIRST` on all SQEs | No change — still fails at same rate |
| POLL_ADD for EAGAIN retry (one-shot read, multi-shot write) | No change — still fails |
| Unbounded MPSC prepare queue | Prevented silent drops, didn't fix throughput hang |
| strace from start | Always passes — Heisenbug disappears under observation |

## MPSC-only result: 0/10 pass — bug is in the ORIGINAL PR

Disabling direct-submit (forcing all ops through the MPSC queue → event loop) does NOT
fix the hang. **0/10 pass at 80 x 4KB.** This means the bug is in the original PR's
completion dispatch or EAGAIN retry logic, not in our multi-thread submission changes.

Our direct-submit optimization is not the cause. The original PR simply doesn't handle
concurrent send+recv on the same socket with large recv buffers correctly.

## Blocking sockets result: 3/5 pass, 2/5 fail — EAGAIN is NOT the cause

Keeping sockets blocking (no O_NONBLOCK) + skipping the sync try (IsReady returns false)
does NOT fix the hang. The kernel's FAST_POLL should handle all waiting internally —
EAGAIN never reaches userspace. Yet the hang persists.

**This proves the bug is NOT in the EAGAIN retry path.** It's in the completion dispatch:
a CQE arrives but the completion callback is never invoked for the waiting async operation.

## Partial send fix: 17/20 pass (was 3/10)

`ProcessIoUringCompletionSuccessSend` returned false for partial sends, triggering
an EAGAIN-style retry that blocked ThreadPool threads on blocking sockets → deadlock.
Fixed by always returning true for successful partial sends.

Remaining 3/20 failures: `recvCalls=66` (always same number for 80 sends). This is
~270KB = socket buffer size. The recv drains all available data, next recv has nothing,
kernel FAST_POLL should wait... but the CQE never arrives. Likely a kernel-level lost
wakeup in FAST_POLL when data arrives during poll setup.

## Currently investigating

If the hang disappears with MPSC-only, the root cause is in the direct-submit path's
multi-thread interaction with the event loop. If it persists, the bug is in the original
PR's CQE dispatch or completion tracking.

## Test Infrastructure

- Azure VM (20.12.235.226): 2-core, Ubuntu 24.04, kernel 6.17 — primary test bed
- Azure VM (74.249.210.155): 8-core — available for multi-core testing
- WSL2 local: kernel 6.6 — io_uring partially works (small ops pass, scale fails)
- Test program at `/tmp/diag2/` on Azure VMs and `/tmp/localtest/` locally
