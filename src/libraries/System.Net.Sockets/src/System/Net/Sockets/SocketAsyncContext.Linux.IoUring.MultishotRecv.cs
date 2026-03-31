// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace System.Net.Sockets
{
    internal sealed partial class SocketAsyncContext
    {
        // ---- Multishot recv per-socket state ----
        private volatile bool _multishotRecvArmed;
        private int _multishotRecvSlotIndex = -1;
        private ulong _multishotRecvUserData;

        // Pre-received data: CQEs that arrived before a ReceiveAsync call.
        // Stores (bufferId, bytesReceived) pairs. Buffers must be recycled after copy.
        private ConcurrentQueue<MultishotRecvData>? _multishotRecvQueue;

        // The currently pending recv operation waiting for a multishot CQE.
        private AsyncOperation? _multishotRecvPendingOp;

        private struct MultishotRecvData
        {
            public int BufferId;
            public int BytesReceived;
        }

        // Diagnostic counters
        private static int s_multishotRecvArmCount;
        private static int s_multishotRecvCqeCount;
        private static int s_multishotRecvFastPathHits;
        private static int s_multishotRecvQueuedCount;
        private static int s_multishotRecvDequeueCount;
        private static int s_multishotRecvFallbackCount;
        private static long s_lastDiagTicks;

        private static void LogMultishotDiagnostics()
        {
            long now = Environment.TickCount64;
            long last = Volatile.Read(ref s_lastDiagTicks);
            if (now - last > 5000 && Interlocked.CompareExchange(ref s_lastDiagTicks, now, last) == last)
            {
                SocketAsyncEngine.IoUringDiag($"[io_uring-multishot] armed={s_multishotRecvArmCount} cqes={s_multishotRecvCqeCount} fastpath={s_multishotRecvFastPathHits} queued={s_multishotRecvQueuedCount} dequeued={s_multishotRecvDequeueCount} fallback={s_multishotRecvFallbackCount}");
            }
        }

        /// <summary>
        /// Tries to arm multishot recv for this socket. Called on first ReceiveAsync
        /// when the engine supports provided buffer rings.
        /// </summary>
        private bool TryArmMultishotRecv()
        {
            if (_multishotRecvArmed) return true;

            SocketAsyncEngine? engine = Volatile.Read(ref _asyncEngine);
            if (engine is null || !engine.IsMultishotRecvEnabled)
            {
                if (Volatile.Read(ref s_multishotRecvArmCount) == 0)
                    SocketAsyncEngine.IoUringDiag($"[io_uring-multishot] Arm skipped: engine={engine is not null}, multishotEnabled={engine?.IsMultishotRecvEnabled}");
                return false;
            }

            _multishotRecvQueue ??= new ConcurrentQueue<MultishotRecvData>();

            int slotIndex = engine.TryArmMultishotRecv(_socket, this, out ulong userData);
            if (slotIndex < 0) return false;

            _multishotRecvSlotIndex = slotIndex;
            _multishotRecvUserData = userData;
            _multishotRecvArmed = true;
            Interlocked.Increment(ref s_multishotRecvArmCount);
            return true;
        }

        /// <summary>
        /// Called by the engine's DispatchMultishotCompletion when a multishot recv CQE
        /// arrives (CQE_F_MORE set). Routes data to the pending operation or queues it.
        /// </summary>
        internal void HandleMultishotRecvCompletion(int result, int bufferId, SocketAsyncEngine engine)
        {
            if (result < 0)
            {
                // Error on the multishot recv — complete the pending op with error if any
                AsyncOperation? pendingOp = Interlocked.Exchange(ref _multishotRecvPendingOp, null);
                if (pendingOp is not null)
                {
                    // Convert negative io_uring result to errno, then to SocketError
                    Interop.Error palError = Interop.Sys.ConvertErrorPlatformToPal(-result);
                    pendingOp.ErrorCode = SocketPal.GetSocketErrorForErrorCode(palError);
                    pendingOp.AssociatedContext.TryCompleteIoUringOperation(pendingOp);
                }
                // Recycle the buffer if one was provided
                if (bufferId >= 0)
                    engine.RecycleProvidedBuffer(bufferId);
                return;
            }

            // Success: result is bytes received, bufferId identifies the kernel-selected buffer
            Interlocked.Increment(ref s_multishotRecvCqeCount);
            LogMultishotDiagnostics();
            AsyncOperation? pending = Interlocked.Exchange(ref _multishotRecvPendingOp, null);
            if (pending is not null && bufferId >= 0)
            {
                // Fast path: a ReceiveAsync is waiting — copy data directly and complete
                Interlocked.Increment(ref s_multishotRecvFastPathHits);
                CopyProvidedBufferToOperation(pending, engine, bufferId, result);
                engine.RecycleProvidedBuffer(bufferId);
                pending.AssociatedContext.TryCompleteIoUringOperation(pending);
            }
            else if (bufferId >= 0)
            {
                // No pending recv — queue the buffer for the next ReceiveAsync
                Interlocked.Increment(ref s_multishotRecvQueuedCount);
                _multishotRecvQueue ??= new ConcurrentQueue<MultishotRecvData>();
                _multishotRecvQueue.Enqueue(new MultishotRecvData
                {
                    BufferId = bufferId,
                    BytesReceived = result
                });
            }
        }

        /// <summary>
        /// Called when the multishot recv is disarmed (terminal CQE without CQE_F_MORE).
        /// The kernel has stopped delivering CQEs for this socket's recv.
        /// </summary>
        internal void HandleMultishotRecvTerminal(int result, SocketAsyncEngine engine)
        {
            _ = result; _ = engine; // Reserved for future error-specific re-arm logic
            _multishotRecvArmed = false;
            _multishotRecvSlotIndex = -1;
            _multishotRecvUserData = 0;

            // If there's a pending recv, fall back to one-shot submission
            AsyncOperation? pending = Volatile.Read(ref _multishotRecvPendingOp);
            if (pending is not null)
            {
                // Re-try: arm multishot again, or fall back to one-shot
                if (!TryArmMultishotRecv())
                {
                    // Multishot failed — clear pending and let it fall through to one-shot
                    pending = Interlocked.Exchange(ref _multishotRecvPendingOp, null);
                    if (pending is not null)
                    {
                        pending.TryDirectSubmitIoUring(this);
                    }
                }
            }
        }

        /// <summary>
        /// Tries the multishot recv fast path for a ReceiveAsync call.
        /// Returns true if the operation was handled (either completed synchronously
        /// from queued data, or registered as pending for the next CQE).
        /// </summary>
        private bool TryMultishotRecvFastPath(
            BufferMemoryReceiveOperation operation)
        {
            // Only for simple recv (no socket address, no flags capture)
            if (operation.SetReceivedFlags || operation.SocketAddress.Length > 0)
            {
                Interlocked.Increment(ref s_multishotRecvFallbackCount);
                return false;
            }

            SocketAsyncEngine? engine = Volatile.Read(ref _asyncEngine);
            if (engine is null) return false;

            // Try to arm if not already
            if (!_multishotRecvArmed && !TryArmMultishotRecv())
                return false;

            // Check for pre-received data
            if (_multishotRecvQueue is not null && _multishotRecvQueue.TryDequeue(out MultishotRecvData data))
            {
                // Data already available — copy and complete synchronously
                Interlocked.Increment(ref s_multishotRecvDequeueCount);
                CopyProvidedBufferToOperation(operation, engine, data.BufferId, data.BytesReceived);
                engine.RecycleProvidedBuffer(data.BufferId);
                return true; // Caller completes synchronously
            }

            // No data queued — register as pending for next CQE
            // The DispatchMultishotCompletion will pick this up
            Volatile.Write(ref _multishotRecvPendingOp, operation);
            return true; // Caller returns IOPending
        }

        /// <summary>
        /// Copies data from a provided buffer to the operation's user buffer.
        /// </summary>
        private static unsafe void CopyProvidedBufferToOperation(
            AsyncOperation operation,
            SocketAsyncEngine engine,
            int bufferId,
            int bytesReceived)
        {
            byte* src = engine.GetProvidedBuffer(bufferId);
            // Get the operation's destination buffer
            if (operation is BufferMemoryReceiveOperation recvOp)
            {
                int copyLen = Math.Min(bytesReceived, recvOp.Buffer.Length);
                new ReadOnlySpan<byte>(src, copyLen).CopyTo(recvOp.Buffer.Span);
                recvOp.BytesTransferred = copyLen;
                recvOp.ReceivedFlags = SocketFlags.None;
                recvOp.ErrorCode = SocketError.Success;
            }
        }
    }
}
