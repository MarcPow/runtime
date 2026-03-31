// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;

namespace System.Net.Sockets
{
    internal sealed partial class SocketAsyncContext
    {
        // io_uring SQE opcode constants (mirror kernel UAPI values).
        // Duplicated here because IoUringOpcodes is private inside SocketAsyncEngine.
        private const byte IoUringOpSend = 26;
        private const byte IoUringOpRecv = 27;
        private const byte IoUringOpSendMsg = 9;
        private const byte IoUringOpRecvMsg = 10;
        private const byte IoUringOpAccept = 13;
        private const byte IoUringOpConnect = 16;

        // io_uring ioprio flags
        private const ushort IoUringRecvSendPollFirst = 1 << 0;

        // Accept-time flags: SOCK_CLOEXEC | SOCK_NONBLOCK
        private const uint IoUringAcceptFlags = 0x80800;

        /// <summary>Returns whether this context's engine is using io_uring completion mode.</summary>
        private bool IsIoUringCompletionModeEnabled()
        {
            SocketAsyncEngine? engine = Volatile.Read(ref _asyncEngine);
            return engine is not null && engine.IsIoUringCompletionModeEnabled;
        }

        /// <summary>Sets _isIoUringActive if the registered engine uses io_uring completion mode.</summary>
        partial void LinuxSetIoStrategyAfterRegistration(SocketAsyncEngine engine)
        {
            if (engine.IsIoUringCompletionModeEnabled)
            {
                _isIoUringActive = true;
            }
        }

        /// <summary>
        /// Called from SafeSocketHandle to ensure deferred cancel CQEs are processed.
        /// In the simplified engine, this is a no-op since we use eventfd-based epoll wakeup.
        /// </summary>
        internal void WakeIoUringEventLoopIfNeeded()
        {
            // Touch _asyncEngine to satisfy CA1822 (instance member requirement).
            _ = Volatile.Read(ref _asyncEngine);
        }

        /// <summary>Removes a completed io_uring operation from its queue and signals or dispatches its callback.</summary>
        internal bool TryCompleteIoUringOperation(AsyncOperation operation)
        {
            bool removed =
                operation is ReadOperation readOperation ? _receiveQueue.TryRemoveCompletedOperation(this, readOperation) :
                operation is WriteOperation writeOperation ? _sendQueue.TryRemoveCompletedOperation(this, writeOperation) :
                false;
            if (!removed)
            {
                return false;
            }

            ManualResetEventSlim? e = operation.Event;
            if (e is not null)
            {
                e.Set();
                return true;
            }

            operation.CancellationRegistration.Dispose();
            if (PreferInlineCompletions)
            {
                operation.InvokeCallback(allowPooling: true);
            }
            else
            {
                operation.QueueIoUringCompletionCallback();
            }

            return true;
        }

        /// <summary>Stages an operation for io_uring preparation if completion mode is active.
        /// Attempts direct SQE submission from the caller thread.</summary>
        static partial void LinuxTryStageIoUringOperation(AsyncOperation operation)
        {
            if (operation.Event is null &&
                operation.AssociatedContext.IsIoUringCompletionModeEnabled() &&
                operation.IoUringUserData == 0 &&
                operation.IsInWaitingState())
            {
                operation.TryDirectSubmitIoUring(operation.AssociatedContext);
            }
        }

        partial void LinuxTryDequeuePreAcceptedConnection(AcceptOperation operation, ref bool dequeued)
        {
            _ = _isIoUringActive; // Satisfy CA1822
        }

        partial void LinuxHasBufferedPersistentMultishotRecvData(ref bool hasBuffered)
        {
            _ = _isIoUringActive; // Satisfy CA1822
        }

        partial void LinuxTryConsumeBufferedPersistentMultishotRecvData(Memory<byte> destination, ref bool consumed, ref int bytesTransferred)
        {
            _ = _isIoUringActive; // Satisfy CA1822
        }

        partial void LinuxOnStopAndAbort()
        {
            _ = _isIoUringActive; // Satisfy CA1822
        }

        // ===================================================================
        // AsyncOperation io_uring extensions
        // ===================================================================

        internal abstract partial class AsyncOperation
        {
            /// <summary>Outcome of processing an io_uring CQE, determining the dispatch action.</summary>
            internal enum IoUringCompletionResult
            {
                Completed = 0,
                Pending = 1,
                Canceled = 2,
                Ignored = 3
            }

            /// <summary>Tri-state result from direct (managed) SQE preparation.</summary>
            internal enum IoUringDirectPrepareResult
            {
                Unsupported = 0,
                Prepared = 1,
                PrepareFailed = 2,
            }

            private int _ioUringCompletionCallbackQueued;
            private int _ioUringCompletionDispatchKind;
            private MemoryHandle _ioUringPinnedBuffer;
            private int _ioUringPinnedBufferActive;
            internal ulong IoUringUserData;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            protected void SetIoUringCompletionDispatchKind(IoUringCompletionDispatchKind kind) =>
                _ioUringCompletionDispatchKind = (int)kind;

            /// <summary>Requests kernel cancellation if the flag is set.</summary>
            partial void LinuxRequestIoUringCancellationIfNeeded(bool requestIoUringCancellation)
            {
                _ = IoUringUserData; // Satisfy CA1822
            }

            /// <summary>Untracks this operation.</summary>
            partial void LinuxUntrackIoUringOperation()
            {
                _ = IoUringUserData; // Satisfy CA1822
            }

            /// <summary>Resets all io_uring preparation state.</summary>
            partial void ResetIoUringState()
            {
                ReleasePinnedIoUringBuffer();
                ReleaseIoUringPreparationResourcesCore();
                IoUringUserData = 0;
            }

            internal void QueueIoUringCompletionCallback()
            {
                Debug.Assert(Event == null);
                if (Interlocked.Exchange(ref _ioUringCompletionCallbackQueued, 1) != 0)
                {
                    Debug.Fail("io_uring completion callback was already queued for this operation.");
                    return;
                }

                // Queue a static callback rather than `this` as IThreadPoolWorkItem,
                // because the derived Execute() calls ProcessAsyncOperation which
                // expects the operation to be at the head of its queue. io_uring
                // completions have already been removed from the queue.
                ThreadPool.UnsafeQueueUserWorkItem(static state =>
                {
                    AsyncOperation op = (AsyncOperation)state!;
                    Interlocked.Exchange(ref op._ioUringCompletionCallbackQueued, 0);
                    op.InvokeCallback(allowPooling: true);
                }, this, preferLocal: false);
            }

            /// <summary>Processes a CQE result and returns the dispatch action for the completion handler.</summary>
            internal IoUringCompletionResult ProcessIoUringCompletionResult(int result, uint flags, uint auxiliaryData)
            {
                _ = auxiliaryData; // Reserved for future use
                Trace($"Enter, result={result}, flags={flags}");

                State oldState = Interlocked.CompareExchange(ref _state, State.Running, State.Waiting);
                if (oldState == State.Canceled)
                {
                    Trace("Exit, previously canceled");
                    return IoUringCompletionResult.Canceled;
                }

                if (oldState != State.Waiting)
                {
                    Trace("Exit, ignored");
                    return IoUringCompletionResult.Ignored;
                }

                if (ProcessIoUringCompletionViaDiscriminator(AssociatedContext, result, auxiliaryData))
                {
                    _state = State.Complete;
                    Trace("Exit, completed");
                    return IoUringCompletionResult.Completed;
                }

                // Incomplete path (e.g. partial send): transition back to Waiting or Canceled.
                State newState;
                while (true)
                {
                    State state = _state;
                    Debug.Assert(state is State.Running or State.RunningWithPendingCancellation);

                    newState = (state == State.Running ? State.Waiting : State.Canceled);
                    if (state == Interlocked.CompareExchange(ref _state, newState, state))
                    {
                        break;
                    }
                }

                if (newState == State.Canceled)
                {
                    ProcessCancellation();
                    Trace("Exit, canceled while pending");
                    return IoUringCompletionResult.Canceled;
                }

                Trace("Exit, pending");
                return IoUringCompletionResult.Pending;
            }

            /// <summary>Releases preparation resources and resets the user_data to zero.</summary>
            internal void ClearIoUringUserData()
            {
                ReleasePinnedIoUringBuffer();
                ReleaseIoUringPreparationResourcesCore();
                IoUringUserData = 0;
            }

            /// <summary>Queues this operation for re-preparation via direct submit.</summary>
            internal bool TryQueueIoUringPreparation()
            {
                if (!AssociatedContext.IsIoUringCompletionModeEnabled())
                {
                    return false;
                }

                // Re-submit directly from ThreadPool thread.
                TryDirectSubmitIoUring(AssociatedContext);
                return IoUringUserData != 0;
            }

            /// <summary>Returns whether this operation is currently in the waiting state.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool IsInWaitingState() => _state == State.Waiting;

            /// <summary>
            /// Attempts to prepare and submit an SQE directly from the calling thread.
            /// </summary>
            internal void TryDirectSubmitIoUring(SocketAsyncContext context)
            {
                SocketAsyncEngine? engine = Volatile.Read(ref context._asyncEngine);
                if (engine is null || !engine.IsIoUringDirectSqeEnabled)
                    return;

                ReleasePinnedIoUringBuffer();
                ReleaseIoUringPreparationResourcesCore();

                IoUringDirectPrepareResult directResult = IoUringPrepareDirect(context, engine, out ulong directUserData);
                if (directResult == IoUringDirectPrepareResult.Prepared && ErrorCode == SocketError.Success)
                {
                    IoUringUserData = directUserData;
                    // FinishSubmission was already called inside IoUringPrepareDirect.
                }
            }

            /// <summary>Prepares an SQE via the direct path. Override in subclasses.</summary>
            protected virtual IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;
                return IoUringDirectPrepareResult.Unsupported;
            }

            /// <summary>Routes a CQE using an operation-kind discriminator.</summary>
            private bool ProcessIoUringCompletionViaDiscriminator(SocketAsyncContext context, int result, uint auxiliaryData)
            {
                _ = auxiliaryData; // Reserved for future use
                IoUringCompletionDispatchKind kind = GetIoUringCompletionDispatchKind();
                if (result >= 0)
                {
                    return kind switch
                    {
                        IoUringCompletionDispatchKind.BufferListSendOperation => ((BufferListSendOperation)this).ProcessIoUringCompletionSuccessBufferListSend(result),
                        IoUringCompletionDispatchKind.BufferMemoryReceiveOperation => ((BufferMemoryReceiveOperation)this).ProcessIoUringCompletionSuccessBufferMemoryReceive(result),
                        IoUringCompletionDispatchKind.BufferListReceiveOperation => ((BufferListReceiveOperation)this).ProcessIoUringCompletionSuccessBufferListReceive(result),
                        IoUringCompletionDispatchKind.ReceiveMessageFromOperation => ((ReceiveMessageFromOperation)this).ProcessIoUringCompletionSuccessReceiveMessageFrom(result),
                        IoUringCompletionDispatchKind.AcceptOperation => ((AcceptOperation)this).ProcessIoUringCompletionSuccessAccept(result),
                        IoUringCompletionDispatchKind.ConnectOperation => ((ConnectOperation)this).ProcessIoUringCompletionSuccessConnect(context),
                        IoUringCompletionDispatchKind.SendOperation => ((SendOperation)this).ProcessIoUringCompletionSuccessSend(result),
                        _ => ProcessIoUringCompletionSuccessDefault(result)
                    };
                }

                return kind switch
                {
                    IoUringCompletionDispatchKind.ReceiveMessageFromOperation => ((ReceiveMessageFromOperation)this).ProcessIoUringCompletionErrorReceiveMessageFrom(result),
                    IoUringCompletionDispatchKind.AcceptOperation => ((AcceptOperation)this).ProcessIoUringCompletionErrorAccept(result),
                    IoUringCompletionDispatchKind.ConnectOperation => ((ConnectOperation)this).ProcessIoUringCompletionErrorConnect(context, result),
                    IoUringCompletionDispatchKind.ReadOperation or
                    IoUringCompletionDispatchKind.BufferMemoryReceiveOperation or
                    IoUringCompletionDispatchKind.BufferListReceiveOperation => ((ReadOperation)this).ProcessIoUringCompletionErrorRead(result),
                    IoUringCompletionDispatchKind.WriteOperation or
                    IoUringCompletionDispatchKind.SendOperation or
                    IoUringCompletionDispatchKind.BufferListSendOperation => ((WriteOperation)this).ProcessIoUringCompletionErrorWrite(result),
                    _ => ProcessIoUringCompletionErrorDefault(result)
                };
            }

            private bool ProcessIoUringCompletionSuccessDefault(int result)
            {
                Debug.Assert(result >= 0);
                ErrorCode = SocketError.Success;
                return true;
            }

            private bool ProcessIoUringCompletionErrorDefault(int result)
            {
                Debug.Assert(result < 0);
                ErrorCode = SocketPal.GetSocketErrorForErrorCode(GetIoUringPalError(result));
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private IoUringCompletionDispatchKind GetIoUringCompletionDispatchKind()
            {
                int dispatchKind = _ioUringCompletionDispatchKind;
                return dispatchKind != 0 ?
                    (IoUringCompletionDispatchKind)dispatchKind :
                    IoUringCompletionDispatchKind.Default;
            }

            /// <summary>Returns whether the negative result represents EAGAIN/EWOULDBLOCK.</summary>
            protected static bool IsIoUringRetryableError(int result)
            {
                if (result >= 0) return false;
                Interop.Error error = GetIoUringPalError(result);
                return error == Interop.Error.EAGAIN || error == Interop.Error.EWOULDBLOCK;
            }

            /// <summary>Converts a negative io_uring result to a SocketError, returning false for retryable errors.</summary>
            protected static bool ProcessIoUringErrorResult(int result, out SocketError errorCode)
            {
                Debug.Assert(result < 0);
                if (IsIoUringRetryableError(result))
                {
                    errorCode = SocketError.Success;
                    return false;
                }

                errorCode = SocketPal.GetSocketErrorForErrorCode(GetIoUringPalError(result));
                return true;
            }

            /// <summary>Converts a negative io_uring CQE result (raw -errno) to PAL error space.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            protected static Interop.Error GetIoUringPalError(int result)
            {
                Debug.Assert(result < 0);
                int platformErrno = -result;
                return Interop.Sys.ConvertErrorPlatformToPal(platformErrno);
            }

            /// <summary>Pins a buffer and returns the raw pointer.</summary>
            protected unsafe byte* PinIoUringBuffer(Memory<byte> buffer)
            {
                ReleasePinnedIoUringBuffer();
                if (buffer.Length == 0) return null;

                _ioUringPinnedBuffer = buffer.Pin();
                Volatile.Write(ref _ioUringPinnedBufferActive, 1);
                return (byte*)_ioUringPinnedBuffer.Pointer;
            }

            /// <summary>Attempts to pin a buffer.</summary>
            protected unsafe bool TryPinIoUringBuffer(Memory<byte> buffer, out byte* pinnedBuffer)
            {
                try
                {
                    pinnedBuffer = PinIoUringBuffer(buffer);
                    if (buffer.Length > 0 && pinnedBuffer is null)
                    {
                        ReleasePinnedIoUringBuffer();
                        ErrorCode = SocketError.Success;
                        return false;
                    }

                    return true;
                }
                catch (NotSupportedException)
                {
                    pinnedBuffer = null;
                    ErrorCode = SocketError.Success;
                    return false;
                }
            }

            /// <summary>Releases the currently pinned buffer handle if active.</summary>
            private void ReleasePinnedIoUringBuffer()
            {
                if (Interlocked.Exchange(ref _ioUringPinnedBufferActive, 0) != 0)
                {
                    _ioUringPinnedBuffer.Dispose();
                    _ioUringPinnedBuffer = default;
                }
            }

            /// <summary>Subclass hook to release operation-specific preparation resources.</summary>
            protected virtual void ReleaseIoUringPreparationResourcesCore()
            {
            }

            /// <summary>Converts SocketFlags to kernel msg_flags for io_uring.</summary>
            protected static bool TryConvertSocketFlags(SocketFlags flags, out uint rwFlags)
            {
                const SocketFlags SupportedFlags =
                    SocketFlags.OutOfBand |
                    SocketFlags.Peek |
                    SocketFlags.DontRoute;

                if ((flags & ~SupportedFlags) != 0)
                {
                    rwFlags = 0;
                    return false;
                }

                rwFlags = (uint)(int)flags;
                return true;
            }

            /// <summary>Pins a socket address buffer.</summary>
            protected static unsafe bool TryPinIoUringSocketAddress(
                Memory<byte> socketAddress,
                ref MemoryHandle pinnedSocketAddress,
                ref int pinnedSocketAddressActive,
                out byte* rawSocketAddress)
            {
                rawSocketAddress = null;
                if (socketAddress.Length == 0) return true;

                if (Volatile.Read(ref pinnedSocketAddressActive) != 0)
                {
                    rawSocketAddress = (byte*)pinnedSocketAddress.Pointer;
                    return rawSocketAddress is not null;
                }

                try
                {
                    pinnedSocketAddress = socketAddress.Pin();
                    Volatile.Write(ref pinnedSocketAddressActive, 1);
                }
                catch (NotSupportedException)
                {
                    return false;
                }

                rawSocketAddress = (byte*)pinnedSocketAddress.Pointer;
                if (rawSocketAddress is null)
                {
                    pinnedSocketAddress.Dispose();
                    pinnedSocketAddress = default;
                    Volatile.Write(ref pinnedSocketAddressActive, 0);
                    return false;
                }

                return true;
            }

            /// <summary>Releases pinned socket-address buffer and message-header allocation.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            protected static unsafe void ReleaseIoUringSocketAddressAndMessageHeader(
                ref MemoryHandle pinnedSocketAddress,
                ref int pinnedSocketAddressActive,
                ref IntPtr messageHeader)
            {
                if (Interlocked.Exchange(ref pinnedSocketAddressActive, 0) != 0)
                {
                    pinnedSocketAddress.Dispose();
                    pinnedSocketAddress = default;
                }

                IntPtr header = Interlocked.Exchange(ref messageHeader, IntPtr.Zero);
                if (header != IntPtr.Zero)
                {
                    NativeMemory.Free((void*)header);
                }
            }

            /// <summary>Frees GCHandles used for buffer list pinning.</summary>
            protected static void ReleasePinnedHandles(GCHandle[] pinnedHandles, int count)
            {
                if (count <= 0) return;
                int releaseCount = count < pinnedHandles.Length ? count : pinnedHandles.Length;
                for (int i = 0; i < releaseCount; i++)
                {
                    if (pinnedHandles[i].IsAllocated) pinnedHandles[i].Free();
                }
            }

            /// <summary>Releases pinned handles and returns arrays to pool.</summary>
            protected static void ReleaseIoUringPinnedHandlesAndIovecs(
                ref GCHandle[]? pinnedHandles,
                ref Interop.Sys.IOVector[]? iovecs,
                ref int pinnedHandleCount)
            {
                GCHandle[]? handles = Interlocked.Exchange(ref pinnedHandles, null);
                int handleCount = Interlocked.Exchange(ref pinnedHandleCount, 0);
                if (handles is not null)
                {
                    ReleasePinnedHandles(handles, handleCount);
                    if (handles.Length != 0) ArrayPool<GCHandle>.Shared.Return(handles, clearArray: true);
                }

                Interop.Sys.IOVector[]? vectors = Interlocked.Exchange(ref iovecs, null);
                if (vectors is not null && vectors.Length != 0)
                {
                    ArrayPool<Interop.Sys.IOVector>.Shared.Return(vectors, clearArray: true);
                }
            }

            /// <summary>Pins a list of buffer segments and builds an iovec array.</summary>
            protected static unsafe bool TryPinBufferListForIoUring(
                IList<ArraySegment<byte>> buffers,
                int startIndex,
                int startOffset,
                out GCHandle[] pinnedHandles,
                out Interop.Sys.IOVector[] iovecs,
                out int iovCount,
                out int pinnedHandleCount,
                out SocketError errorCode)
            {
                iovCount = 0;
                pinnedHandleCount = 0;
                if ((uint)startIndex > (uint)buffers.Count)
                {
                    errorCode = SocketError.InvalidArgument;
                    pinnedHandles = Array.Empty<GCHandle>();
                    iovecs = Array.Empty<Interop.Sys.IOVector>();
                    return false;
                }

                int remainingBufferCount = buffers.Count - startIndex;
                pinnedHandles = remainingBufferCount == 0 ? Array.Empty<GCHandle>() : ArrayPool<GCHandle>.Shared.Rent(remainingBufferCount);
                iovecs = remainingBufferCount == 0 ? Array.Empty<Interop.Sys.IOVector>() : ArrayPool<Interop.Sys.IOVector>.Shared.Rent(remainingBufferCount);

                int currentOffset = startOffset;
                byte[]? lastPinnedArray = null;
                GCHandle lastPinnedHandle = default;
                try
                {
                    for (int i = 0; i < remainingBufferCount; i++, currentOffset = 0)
                    {
                        ArraySegment<byte> buffer = buffers[startIndex + i];
                        RangeValidationHelpers.ValidateSegment(buffer);

                        if ((uint)currentOffset > (uint)buffer.Count)
                        {
                            ReleasePinnedHandles(pinnedHandles, pinnedHandleCount);
                            if (pinnedHandles.Length != 0) ArrayPool<GCHandle>.Shared.Return(pinnedHandles, clearArray: true);
                            if (iovecs.Length != 0) ArrayPool<Interop.Sys.IOVector>.Shared.Return(iovecs, clearArray: true);
                            errorCode = SocketError.InvalidArgument;
                            return false;
                        }

                        int bufferCount = buffer.Count - currentOffset;
                        byte* basePtr = null;
                        if (bufferCount != 0)
                        {
                            byte[] array = buffer.Array!;
                            GCHandle handle;
                            if (ReferenceEquals(array, lastPinnedArray))
                            {
                                handle = lastPinnedHandle;
                            }
                            else
                            {
                                handle = GCHandle.Alloc(array, GCHandleType.Pinned);
                                pinnedHandles[pinnedHandleCount] = handle;
                                pinnedHandleCount++;
                                lastPinnedArray = array;
                                lastPinnedHandle = handle;
                            }

                            basePtr = &((byte*)handle.AddrOfPinnedObject())[buffer.Offset + currentOffset];
                        }

                        iovecs[i].Base = basePtr;
                        iovecs[i].Count = (UIntPtr)bufferCount;
                        iovCount++;
                    }
                }
                catch
                {
                    ReleasePinnedHandles(pinnedHandles, pinnedHandleCount);
                    if (pinnedHandles.Length != 0) ArrayPool<GCHandle>.Shared.Return(pinnedHandles, clearArray: true);
                    if (iovecs.Length != 0) ArrayPool<Interop.Sys.IOVector>.Shared.Return(iovecs, clearArray: true);
                    throw;
                }

                errorCode = SocketError.Success;
                return true;
            }

            /// <summary>Returns the epoll event mask to use when falling back from io_uring to readiness notification.</summary>
            internal virtual Interop.Sys.SocketEvents GetIoUringFallbackSocketEvents() =>
                Interop.Sys.SocketEvents.None;
        }

        // ===================================================================
        // Per-operation-type io_uring extensions
        // ===================================================================

        internal abstract partial class ReadOperation
        {
            internal bool ProcessIoUringCompletionErrorRead(int result) =>
                ProcessIoUringErrorResult(result, out ErrorCode);

            internal override Interop.Sys.SocketEvents GetIoUringFallbackSocketEvents() =>
                Interop.Sys.SocketEvents.Read;
        }

        private abstract partial class WriteOperation
        {
            internal bool ProcessIoUringCompletionErrorWrite(int result) =>
                ProcessIoUringErrorResult(result, out ErrorCode);

            internal override Interop.Sys.SocketEvents GetIoUringFallbackSocketEvents() =>
                Interop.Sys.SocketEvents.Write;
        }

        private abstract partial class SendOperation
        {
            internal bool ProcessIoUringCompletionSuccessSend(int result)
            {
                if (result == 0)
                {
                    if (Count > 0)
                    {
                        ErrorCode = SocketError.ConnectionReset;
                        return true;
                    }

                    ErrorCode = SocketError.Success;
                    return true;
                }

                Debug.Assert(result > 0);
                int sent = Math.Min(result, Count);
                BytesTransferred += sent;
                Offset += sent;
                Count -= sent;
                ErrorCode = SocketError.Success;
                return true;
            }
        }

        private partial class BufferMemorySendOperation
        {
            private IntPtr _ioUringMessageHeader;
            private MemoryHandle _ioUringPinnedSocketAddress;
            private int _ioUringPinnedSocketAddressActive;

            protected override unsafe void ReleaseIoUringPreparationResourcesCore()
            {
                ReleaseIoUringSocketAddressAndMessageHeader(
                    ref _ioUringPinnedSocketAddress,
                    ref _ioUringPinnedSocketAddressActive,
                    ref _ioUringMessageHeader);
            }

            protected override unsafe IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;

                if (!TryPinIoUringBuffer(Buffer, out byte* rawBuffer))
                    return IoUringDirectPrepareResult.PrepareFailed;

                if (rawBuffer is not null)
                    rawBuffer += Offset;

                if (!TryConvertSocketFlags(Flags, out uint rwFlags))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                if (SocketAddress.Length == 0)
                {
                    // Simple send (no destination address)
                    var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpSend);
                    if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                    {
                        ErrorCode = setup.ErrorCode;
                        return setup.PrepareResult;
                    }

                    // Write SQE fields directly (mirrors WriteSendLikeSqe)
                    SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                    sqe->Opcode = IoUringOpSend;
                    sqe->Flags = setup.SqeFlags;
                    sqe->Ioprio = IoUringRecvSendPollFirst;
                    sqe->Fd = setup.SqeFd;
                    sqe->Off = 0;
                    sqe->Addr = (ulong)(nuint)rawBuffer;
                    sqe->Len = (uint)Count;
                    sqe->RwFlags = rwFlags;
                    sqe->UserData = setup.UserData;

                    engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                    userData = setup.UserData;
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.Prepared;
                }

                // SendTo with destination address — use sendmsg
                if (!TryPinIoUringSocketAddress(
                    SocketAddress,
                    ref _ioUringPinnedSocketAddress,
                    ref _ioUringPinnedSocketAddressActive,
                    out byte* rawSocketAddress))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                Interop.Sys.MessageHeader* messageHeader = GetOrCreateMessageHeader(rawSocketAddress);
                Interop.Sys.IOVector sendIov;
                sendIov.Base = rawBuffer;
                sendIov.Count = (UIntPtr)Count;
                if (Count == 0)
                {
                    messageHeader->IOVectors = null;
                    messageHeader->IOVectorCount = 0;
                }
                else
                {
                    messageHeader->IOVectors = &sendIov;
                    messageHeader->IOVectorCount = 1;
                }

                var msgSetup = engine.TrySetupDirectSqe(context._socket, IoUringOpSendMsg);
                if (msgSetup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = msgSetup.ErrorCode;
                    return msgSetup.PrepareResult;
                }

                // Write SQE fields (mirrors WriteSendMsgLikeSqe)
                SocketAsyncEngine.IoUringSqe* msgSqe = msgSetup.Sqe;
                msgSqe->Opcode = IoUringOpSendMsg;
                msgSqe->Flags = msgSetup.SqeFlags;
                msgSqe->Ioprio = IoUringRecvSendPollFirst;
                msgSqe->Fd = msgSetup.SqeFd;
                msgSqe->Off = 0;
                msgSqe->Addr = (ulong)(nuint)messageHeader;
                msgSqe->Len = 1;
                msgSqe->RwFlags = rwFlags;
                msgSqe->UserData = msgSetup.UserData;

                engine.FinishSubmission(msgSetup.SlotIndex, msgSetup.UserData, this);
                userData = msgSetup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            private unsafe Interop.Sys.MessageHeader* GetOrCreateMessageHeader(byte* rawSocketAddress)
            {
                Interop.Sys.MessageHeader* messageHeader = (Interop.Sys.MessageHeader*)_ioUringMessageHeader;
                if (messageHeader is null)
                {
                    messageHeader = (Interop.Sys.MessageHeader*)NativeMemory.Alloc((nuint)sizeof(Interop.Sys.MessageHeader));
                    _ioUringMessageHeader = (IntPtr)messageHeader;
                }

                messageHeader->SocketAddress = rawSocketAddress;
                messageHeader->SocketAddressLen = SocketAddress.Length;
                messageHeader->ControlBuffer = null;
                messageHeader->ControlBufferLen = 0;
                messageHeader->Flags = SocketFlags.None;
                return messageHeader;
            }
        }

        private sealed partial class BufferListSendOperation
        {
            private GCHandle[]? _ioUringPinnedBufferHandles;
            private Interop.Sys.IOVector[]? _ioUringIovecs;
            private int _ioUringPinnedHandleCount;
            private int _ioUringPreparedBufferCount = -1;
            private int _ioUringPreparedStartIndex = -1;
            private int _ioUringPreparedStartOffset = -1;
            private int _ioUringPreparedIovCount;

            protected override void ReleaseIoUringPreparationResourcesCore()
            {
                ReleaseIoUringPinnedHandlesAndIovecs(ref _ioUringPinnedBufferHandles, ref _ioUringIovecs, ref _ioUringPinnedHandleCount);
                _ioUringPreparedBufferCount = -1;
                _ioUringPreparedStartIndex = -1;
                _ioUringPreparedStartOffset = -1;
                _ioUringPreparedIovCount = 0;
            }

            private bool TryPinIoUringBuffers(
                IList<ArraySegment<byte>> buffers,
                int startIndex,
                int startOffset,
                out int iovCount)
            {
                if (_ioUringPinnedBufferHandles is not null &&
                    _ioUringIovecs is not null &&
                    _ioUringPreparedBufferCount == buffers.Count &&
                    _ioUringPreparedStartIndex == startIndex &&
                    _ioUringPreparedStartOffset == startOffset &&
                    _ioUringPreparedIovCount <= _ioUringIovecs.Length)
                {
                    iovCount = _ioUringPreparedIovCount;
                    return true;
                }

                ReleaseIoUringPinnedHandlesAndIovecs(ref _ioUringPinnedBufferHandles, ref _ioUringIovecs, ref _ioUringPinnedHandleCount);

                if (!TryPinBufferListForIoUring(
                        buffers, startIndex, startOffset,
                        out GCHandle[] pinnedHandles,
                        out Interop.Sys.IOVector[] iovecs,
                        out iovCount,
                        out int pinnedHandleCount,
                        out SocketError errorCode))
                {
                    ErrorCode = errorCode;
                    return false;
                }

                _ioUringPinnedBufferHandles = pinnedHandles;
                _ioUringIovecs = iovecs;
                _ioUringPinnedHandleCount = pinnedHandleCount;
                _ioUringPreparedBufferCount = buffers.Count;
                _ioUringPreparedStartIndex = startIndex;
                _ioUringPreparedStartOffset = startOffset;
                _ioUringPreparedIovCount = iovCount;
                return true;
            }

            private bool AdvanceSendBufferPosition(int bytesSent)
            {
                IList<ArraySegment<byte>>? buffers = Buffers;
                if (buffers is null || bytesSent <= 0)
                    return buffers is null || BufferIndex >= buffers.Count;

                int remaining = bytesSent;
                int index = BufferIndex;
                int offset = Offset;

                while (remaining > 0 && index < buffers.Count)
                {
                    int available = buffers[index].Count - offset;
                    if (available > remaining)
                    {
                        offset += remaining;
                        break;
                    }

                    remaining -= Math.Max(available, 0);
                    index++;
                    offset = 0;
                }

                BufferIndex = index;
                Offset = offset;
                return index >= buffers.Count;
            }

            protected override unsafe IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;
                IList<ArraySegment<byte>>? buffers = Buffers;
                if (buffers is null)
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                if (!TryPinIoUringBuffers(buffers, BufferIndex, Offset, out int iovCount))
                    return IoUringDirectPrepareResult.PrepareFailed;

                if (!TryConvertSocketFlags(Flags, out uint rwFlags))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                byte* rawSocketAddress = null;
                if (SocketAddress.Length != 0 && !TryPinIoUringBuffer(SocketAddress, out rawSocketAddress))
                    return IoUringDirectPrepareResult.PrepareFailed;

                // Use sendmsg for buffer-list sends
                Interop.Sys.MessageHeader messageHeader;
                messageHeader.SocketAddress = rawSocketAddress;
                messageHeader.SocketAddressLen = SocketAddress.Length;
                messageHeader.ControlBuffer = null;
                messageHeader.ControlBufferLen = 0;
                messageHeader.Flags = SocketFlags.None;

                Interop.Sys.IOVector[] iovecs = _ioUringIovecs!;
                if (iovCount != 0)
                {
                    fixed (Interop.Sys.IOVector* iovecsPtr = &iovecs[0])
                    {
                        messageHeader.IOVectors = iovecsPtr;
                        messageHeader.IOVectorCount = iovCount;

                        var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpSendMsg);
                        if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                        {
                            ErrorCode = setup.ErrorCode;
                            return setup.PrepareResult;
                        }

                        SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                        sqe->Opcode = IoUringOpSendMsg;
                        sqe->Flags = setup.SqeFlags;
                        sqe->Ioprio = IoUringRecvSendPollFirst;
                        sqe->Fd = setup.SqeFd;
                        sqe->Off = 0;
                        sqe->Addr = (ulong)(nuint)(&messageHeader);
                        sqe->Len = 1;
                        sqe->RwFlags = rwFlags;
                        sqe->UserData = setup.UserData;

                        engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                        userData = setup.UserData;
                        ErrorCode = SocketError.Success;
                        return IoUringDirectPrepareResult.Prepared;
                    }
                }

                // Empty buffer list
                messageHeader.IOVectors = null;
                messageHeader.IOVectorCount = 0;
                var emptySetup = engine.TrySetupDirectSqe(context._socket, IoUringOpSendMsg);
                if (emptySetup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = emptySetup.ErrorCode;
                    return emptySetup.PrepareResult;
                }

                SocketAsyncEngine.IoUringSqe* emptySqe = emptySetup.Sqe;
                emptySqe->Opcode = IoUringOpSendMsg;
                emptySqe->Flags = emptySetup.SqeFlags;
                emptySqe->Ioprio = IoUringRecvSendPollFirst;
                emptySqe->Fd = emptySetup.SqeFd;
                emptySqe->Off = 0;
                emptySqe->Addr = (ulong)(nuint)(&messageHeader);
                emptySqe->Len = 1;
                emptySqe->RwFlags = rwFlags;
                emptySqe->UserData = emptySetup.UserData;

                engine.FinishSubmission(emptySetup.SlotIndex, emptySetup.UserData, this);
                userData = emptySetup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            internal bool ProcessIoUringCompletionSuccessBufferListSend(int result)
            {
                if (result == 0)
                {
                    if (HasPendingBufferListSendBytes())
                    {
                        ErrorCode = SocketError.ConnectionReset;
                        return true;
                    }

                    ErrorCode = SocketError.Success;
                    return true;
                }

                Debug.Assert(result > 0);
                BytesTransferred += result;
                bool complete = AdvanceSendBufferPosition(result);
                ErrorCode = SocketError.Success;
                return complete;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool HasPendingBufferListSendBytes()
            {
                IList<ArraySegment<byte>>? buffers = Buffers;
                if (buffers is null || BufferIndex >= buffers.Count)
                    return false;

                int index = BufferIndex;
                int offset = Offset;
                while (index < buffers.Count)
                {
                    int available = buffers[index].Count - offset;
                    if (available > 0) return true;
                    index++;
                    offset = 0;
                }

                return false;
            }
        }

        private sealed partial class BufferMemoryReceiveOperation
        {
            private IntPtr _ioUringMessageHeader;
            private MemoryHandle _ioUringPinnedSocketAddress;
            private int _ioUringPinnedSocketAddressActive;

            protected override unsafe void ReleaseIoUringPreparationResourcesCore()
            {
                ReleaseIoUringSocketAddressAndMessageHeader(
                    ref _ioUringPinnedSocketAddress,
                    ref _ioUringPinnedSocketAddressActive,
                    ref _ioUringMessageHeader);
            }

            protected override unsafe IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;

                if (!TryPinIoUringBuffer(Buffer, out byte* rawBuffer))
                    return IoUringDirectPrepareResult.PrepareFailed;

                if (!TryConvertSocketFlags(Flags, out uint rwFlags))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                if (SetReceivedFlags || SocketAddress.Length != 0)
                {
                    // recvmsg path (need socket address or msg_flags)
                    return IoUringPrepareDirectReceiveMessage(context, engine, rawBuffer, rwFlags, out userData);
                }

                // Simple recv (connected socket, no flags needed)
                var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpRecv);
                if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = setup.ErrorCode;
                    return setup.PrepareResult;
                }

                SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                sqe->Opcode = IoUringOpRecv;
                sqe->Flags = setup.SqeFlags;
                sqe->Ioprio = IoUringRecvSendPollFirst;
                sqe->Fd = setup.SqeFd;
                sqe->Off = 0;
                sqe->Addr = (ulong)(nuint)rawBuffer;
                sqe->Len = (uint)Buffer.Length;
                sqe->RwFlags = rwFlags;
                sqe->UserData = setup.UserData;

                engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                userData = setup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            private unsafe IoUringDirectPrepareResult IoUringPrepareDirectReceiveMessage(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                byte* rawBuffer,
                uint rwFlags,
                out ulong userData)
            {
                userData = 0;

                if (!TryPinIoUringSocketAddress(
                    SocketAddress,
                    ref _ioUringPinnedSocketAddress,
                    ref _ioUringPinnedSocketAddressActive,
                    out byte* rawSocketAddress))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                Interop.Sys.MessageHeader* messageHeader = (Interop.Sys.MessageHeader*)_ioUringMessageHeader;
                if (messageHeader is null)
                {
                    messageHeader = (Interop.Sys.MessageHeader*)NativeMemory.Alloc((nuint)sizeof(Interop.Sys.MessageHeader));
                    _ioUringMessageHeader = (IntPtr)messageHeader;
                }

                messageHeader->SocketAddress = rawSocketAddress;
                messageHeader->SocketAddressLen = SocketAddress.Length;
                messageHeader->ControlBuffer = null;
                messageHeader->ControlBufferLen = 0;
                messageHeader->Flags = SocketFlags.None;

                Interop.Sys.IOVector receiveIov;
                receiveIov.Base = rawBuffer;
                receiveIov.Count = (UIntPtr)Buffer.Length;
                messageHeader->IOVectors = &receiveIov;
                messageHeader->IOVectorCount = 1;

                var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpRecvMsg);
                if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = setup.ErrorCode;
                    return setup.PrepareResult;
                }

                SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                sqe->Opcode = IoUringOpRecvMsg;
                sqe->Flags = setup.SqeFlags;
                sqe->Ioprio = IoUringRecvSendPollFirst;
                sqe->Fd = setup.SqeFd;
                sqe->Off = 0;
                sqe->Addr = (ulong)(nuint)messageHeader;
                sqe->Len = 1;
                sqe->RwFlags = rwFlags;
                sqe->UserData = setup.UserData;

                engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                userData = setup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            internal bool ProcessIoUringCompletionSuccessBufferMemoryReceive(int result)
            {
                BytesTransferred = result;
                ReceivedFlags = SocketFlags.None;
                ErrorCode = SocketError.Success;
                return true;
            }
        }

        private sealed partial class BufferListReceiveOperation
        {
            private GCHandle[]? _ioUringPinnedBufferHandles;
            private Interop.Sys.IOVector[]? _ioUringIovecs;
            private int _ioUringPinnedHandleCount;
            private IntPtr _ioUringMessageHeader;
            private int _ioUringPreparedIovCount;
            private int _ioUringPreparedBufferCount = -1;

            protected override unsafe void ReleaseIoUringPreparationResourcesCore()
            {
                ReleaseIoUringPinnedHandlesAndIovecs(ref _ioUringPinnedBufferHandles, ref _ioUringIovecs, ref _ioUringPinnedHandleCount);
                _ioUringPreparedIovCount = 0;
                _ioUringPreparedBufferCount = -1;

                IntPtr messageHeader = Interlocked.Exchange(ref _ioUringMessageHeader, IntPtr.Zero);
                if (messageHeader != IntPtr.Zero)
                {
                    NativeMemory.Free((void*)messageHeader);
                }
            }

            private bool TryPinIoUringBuffers(IList<ArraySegment<byte>> buffers, out int iovCount)
            {
                if (_ioUringPinnedBufferHandles is not null &&
                    _ioUringIovecs is not null &&
                    _ioUringPreparedIovCount != 0 &&
                    _ioUringPreparedIovCount <= _ioUringIovecs.Length &&
                    _ioUringPreparedBufferCount == buffers.Count)
                {
                    iovCount = _ioUringPreparedIovCount;
                    return true;
                }

                ReleaseIoUringPinnedHandlesAndIovecs(ref _ioUringPinnedBufferHandles, ref _ioUringIovecs, ref _ioUringPinnedHandleCount);

                if (!TryPinBufferListForIoUring(
                        buffers, 0, 0,
                        out GCHandle[] pinnedHandles,
                        out Interop.Sys.IOVector[] iovecs,
                        out iovCount,
                        out int pinnedHandleCount,
                        out SocketError errorCode))
                {
                    ErrorCode = errorCode;
                    return false;
                }

                _ioUringPinnedBufferHandles = pinnedHandles;
                _ioUringIovecs = iovecs;
                _ioUringPinnedHandleCount = pinnedHandleCount;
                _ioUringPreparedIovCount = iovCount;
                _ioUringPreparedBufferCount = buffers.Count;
                return true;
            }

            protected override unsafe IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;
                IList<ArraySegment<byte>>? buffers = Buffers;
                if (buffers is null)
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                if (!TryPinIoUringBuffers(buffers, out int iovCount))
                    return IoUringDirectPrepareResult.PrepareFailed;

                if (!TryConvertSocketFlags(Flags, out uint rwFlags))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                byte* rawSocketAddress = null;
                if (SocketAddress.Length != 0 && !TryPinIoUringBuffer(SocketAddress, out rawSocketAddress))
                    return IoUringDirectPrepareResult.PrepareFailed;

                Interop.Sys.MessageHeader* messageHeader = (Interop.Sys.MessageHeader*)_ioUringMessageHeader;
                if (messageHeader is null)
                {
                    messageHeader = (Interop.Sys.MessageHeader*)NativeMemory.Alloc((nuint)sizeof(Interop.Sys.MessageHeader));
                    _ioUringMessageHeader = (IntPtr)messageHeader;
                }

                messageHeader->SocketAddress = rawSocketAddress;
                messageHeader->SocketAddressLen = SocketAddress.Length;
                messageHeader->ControlBuffer = null;
                messageHeader->ControlBufferLen = 0;
                messageHeader->Flags = SocketFlags.None;

                Interop.Sys.IOVector[] iovecs = _ioUringIovecs!;
                if (iovCount != 0)
                {
                    fixed (Interop.Sys.IOVector* iovecsPtr = &iovecs[0])
                    {
                        messageHeader->IOVectors = iovecsPtr;
                        messageHeader->IOVectorCount = iovCount;

                        var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpRecvMsg);
                        if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                        {
                            ErrorCode = setup.ErrorCode;
                            return setup.PrepareResult;
                        }

                        SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                        sqe->Opcode = IoUringOpRecvMsg;
                        sqe->Flags = setup.SqeFlags;
                        sqe->Ioprio = IoUringRecvSendPollFirst;
                        sqe->Fd = setup.SqeFd;
                        sqe->Off = 0;
                        sqe->Addr = (ulong)(nuint)messageHeader;
                        sqe->Len = 1;
                        sqe->RwFlags = rwFlags;
                        sqe->UserData = setup.UserData;

                        engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                        userData = setup.UserData;
                        ErrorCode = SocketError.Success;
                        return IoUringDirectPrepareResult.Prepared;
                    }
                }

                // Empty buffer list
                messageHeader->IOVectors = null;
                messageHeader->IOVectorCount = 0;
                var emptySetup = engine.TrySetupDirectSqe(context._socket, IoUringOpRecvMsg);
                if (emptySetup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = emptySetup.ErrorCode;
                    return emptySetup.PrepareResult;
                }

                SocketAsyncEngine.IoUringSqe* emptySqe = emptySetup.Sqe;
                emptySqe->Opcode = IoUringOpRecvMsg;
                emptySqe->Flags = emptySetup.SqeFlags;
                emptySqe->Ioprio = IoUringRecvSendPollFirst;
                emptySqe->Fd = emptySetup.SqeFd;
                emptySqe->Off = 0;
                emptySqe->Addr = (ulong)(nuint)messageHeader;
                emptySqe->Len = 1;
                emptySqe->RwFlags = 0;
                emptySqe->UserData = emptySetup.UserData;

                engine.FinishSubmission(emptySetup.SlotIndex, emptySetup.UserData, this);
                userData = emptySetup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            internal bool ProcessIoUringCompletionSuccessBufferListReceive(int result)
            {
                BytesTransferred = result;
                ReceivedFlags = SocketFlags.None;
                ErrorCode = SocketError.Success;
                return true;
            }
        }

        private sealed partial class ReceiveMessageFromOperation
        {
            private GCHandle[]? _ioUringPinnedBufferHandles;
            private Interop.Sys.IOVector[]? _ioUringIovecs;
            private int _ioUringPinnedHandleCount;
            private int _ioUringPreparedIovCount;
            private int _ioUringPreparedBufferListCount = -1;
            private IntPtr _ioUringMessageHeader;
            private IntPtr _ioUringControlBuffer;
            private int _ioUringControlBufferLength;
            private MemoryHandle _ioUringPinnedSocketAddress;
            private int _ioUringPinnedSocketAddressActive;

            protected override unsafe void ReleaseIoUringPreparationResourcesCore()
            {
                ReleaseIoUringPinnedHandlesAndIovecs(ref _ioUringPinnedBufferHandles, ref _ioUringIovecs, ref _ioUringPinnedHandleCount);
                _ioUringPreparedIovCount = 0;
                _ioUringPreparedBufferListCount = -1;

                IntPtr controlBuffer = Interlocked.Exchange(ref _ioUringControlBuffer, IntPtr.Zero);
                if (controlBuffer != IntPtr.Zero)
                {
                    NativeMemory.Free((void*)controlBuffer);
                }
                _ioUringControlBufferLength = 0;

                ReleaseIoUringSocketAddressAndMessageHeader(
                    ref _ioUringPinnedSocketAddress,
                    ref _ioUringPinnedSocketAddressActive,
                    ref _ioUringMessageHeader);
            }

            private bool TryPinIoUringBuffers(IList<ArraySegment<byte>> buffers, out int iovCount)
            {
                if (_ioUringPinnedBufferHandles is not null &&
                    _ioUringIovecs is not null &&
                    _ioUringPreparedIovCount <= _ioUringIovecs.Length &&
                    _ioUringPreparedBufferListCount == buffers.Count)
                {
                    iovCount = _ioUringPreparedIovCount;
                    return true;
                }

                ReleaseIoUringPinnedHandlesAndIovecs(ref _ioUringPinnedBufferHandles, ref _ioUringIovecs, ref _ioUringPinnedHandleCount);

                if (!TryPinBufferListForIoUring(
                        buffers, 0, 0,
                        out GCHandle[] pinnedHandles,
                        out Interop.Sys.IOVector[] iovecs,
                        out iovCount,
                        out int pinnedHandleCount,
                        out SocketError errorCode))
                {
                    ErrorCode = errorCode;
                    return false;
                }

                _ioUringPinnedBufferHandles = pinnedHandles;
                _ioUringIovecs = iovecs;
                _ioUringPinnedHandleCount = pinnedHandleCount;
                _ioUringPreparedIovCount = iovCount;
                _ioUringPreparedBufferListCount = buffers.Count;
                return true;
            }

            protected override unsafe IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;
                IList<ArraySegment<byte>>? buffers = Buffers;
                byte* rawBuffer = null;
                int iovCount;

                if (buffers is not null)
                {
                    if (!TryPinIoUringBuffers(buffers, out iovCount))
                        return IoUringDirectPrepareResult.PrepareFailed;
                }
                else
                {
                    if (!TryPinIoUringBuffer(Buffer, out rawBuffer))
                        return IoUringDirectPrepareResult.PrepareFailed;
                    iovCount = 1;
                }

                if (!TryPinIoUringSocketAddress(
                    SocketAddress,
                    ref _ioUringPinnedSocketAddress,
                    ref _ioUringPinnedSocketAddressActive,
                    out byte* rawSocketAddress))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                if (!TryConvertSocketFlags(Flags, out uint rwFlags))
                {
                    ErrorCode = SocketError.Success;
                    return IoUringDirectPrepareResult.PrepareFailed;
                }

                Interop.Sys.MessageHeader* messageHeader = (Interop.Sys.MessageHeader*)_ioUringMessageHeader;
                if (messageHeader is null)
                {
                    messageHeader = (Interop.Sys.MessageHeader*)NativeMemory.Alloc((nuint)sizeof(Interop.Sys.MessageHeader));
                    _ioUringMessageHeader = (IntPtr)messageHeader;
                }

                messageHeader->SocketAddress = rawSocketAddress;
                messageHeader->SocketAddressLen = SocketAddress.Length;
                messageHeader->Flags = SocketFlags.None;

                int controlBufferLen = Interop.Sys.GetControlMessageBufferSize(Convert.ToInt32(IsIPv4), Convert.ToInt32(IsIPv6));
                if (controlBufferLen > 0)
                {
                    if (_ioUringControlBuffer == IntPtr.Zero || _ioUringControlBufferLength != controlBufferLen)
                    {
                        IntPtr oldBuf = Interlocked.Exchange(ref _ioUringControlBuffer, IntPtr.Zero);
                        if (oldBuf != IntPtr.Zero) NativeMemory.Free((void*)oldBuf);

                        _ioUringControlBuffer = (IntPtr)NativeMemory.Alloc((nuint)controlBufferLen);
                        _ioUringControlBufferLength = controlBufferLen;
                    }

                    messageHeader->ControlBuffer = (byte*)_ioUringControlBuffer;
                    messageHeader->ControlBufferLen = controlBufferLen;
                }
                else
                {
                    messageHeader->ControlBuffer = null;
                    messageHeader->ControlBufferLen = 0;
                }

                if (buffers is not null)
                {
                    Interop.Sys.IOVector[] iovecs = _ioUringIovecs!;
                    if (iovCount != 0)
                    {
                        fixed (Interop.Sys.IOVector* iovecsPtr = &iovecs[0])
                        {
                            messageHeader->IOVectors = iovecsPtr;
                            messageHeader->IOVectorCount = iovCount;
                            return SubmitRecvMsgSqe(context, engine, messageHeader, rwFlags, out userData);
                        }
                    }

                    messageHeader->IOVectors = null;
                    messageHeader->IOVectorCount = 0;
                    return SubmitRecvMsgSqe(context, engine, messageHeader, rwFlags, out userData);
                }

                // Single buffer path
                Interop.Sys.IOVector iov;
                iov.Base = rawBuffer;
                iov.Count = (UIntPtr)Buffer.Length;
                messageHeader->IOVectors = &iov;
                messageHeader->IOVectorCount = 1;
                return SubmitRecvMsgSqe(context, engine, messageHeader, rwFlags, out userData);
            }

            private unsafe IoUringDirectPrepareResult SubmitRecvMsgSqe(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                Interop.Sys.MessageHeader* messageHeader,
                uint rwFlags,
                out ulong userData)
            {
                userData = 0;
                var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpRecvMsg);
                if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = setup.ErrorCode;
                    return setup.PrepareResult;
                }

                SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                sqe->Opcode = IoUringOpRecvMsg;
                sqe->Flags = setup.SqeFlags;
                sqe->Ioprio = IoUringRecvSendPollFirst;
                sqe->Fd = setup.SqeFd;
                sqe->Off = 0;
                sqe->Addr = (ulong)(nuint)messageHeader;
                sqe->Len = 1;
                sqe->RwFlags = rwFlags;
                sqe->UserData = setup.UserData;

                engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                userData = setup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            internal unsafe bool ProcessIoUringCompletionSuccessReceiveMessageFrom(int result)
            {
                BytesTransferred = result;
                ReceivedFlags = SocketFlags.None;
                ErrorCode = SocketError.Success;
                IPPacketInformation = default;

                if (_ioUringMessageHeader != IntPtr.Zero)
                {
                    Interop.Sys.MessageHeader* messageHeader = (Interop.Sys.MessageHeader*)_ioUringMessageHeader;

                    if (SocketAddress.Length != 0)
                    {
                        int socketAddressLen = messageHeader->SocketAddressLen;
                        if (socketAddressLen < 0) socketAddressLen = 0;
                        if ((uint)socketAddressLen > (uint)SocketAddress.Length)
                            socketAddressLen = SocketAddress.Length;
                        SocketAddress = SocketAddress.Slice(0, socketAddressLen);
                    }

                    ReceivedFlags = messageHeader->Flags;
                    IPPacketInformation = SocketPal.GetIoUringIPPacketInformation(messageHeader, IsIPv4, IsIPv6);
                }

                return true;
            }

            internal bool ProcessIoUringCompletionErrorReceiveMessageFrom(int result)
            {
                if (!ProcessIoUringErrorResult(result, out ErrorCode))
                    return false;

                IPPacketInformation = default;
                return true;
            }
        }

        internal sealed partial class AcceptOperation
        {
            public int AcceptSocketAddressLength;

            internal override Interop.Sys.SocketEvents GetIoUringFallbackSocketEvents() =>
                Interop.Sys.SocketEvents.Read;

            protected override unsafe IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;
                AcceptSocketAddressLength = SocketAddress.Length;

                if (!TryPinIoUringBuffer(SocketAddress, out byte* rawSocketAddress))
                    return IoUringDirectPrepareResult.PrepareFailed;

                // Pin a stackalloc int for the socklen_t output parameter
                int socketAddressLen = SocketAddress.Length;

                var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpAccept);
                if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = setup.ErrorCode;
                    return setup.PrepareResult;
                }

                SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                sqe->Opcode = IoUringOpAccept;
                sqe->Flags = setup.SqeFlags;
                sqe->Ioprio = 0;
                sqe->Fd = setup.SqeFd;
                sqe->Addr = (ulong)(nuint)rawSocketAddress;
                sqe->Off = 0; // No socklen_t pointer — kernel will fill in the address
                sqe->Len = 0;
                sqe->RwFlags = IoUringAcceptFlags;
                sqe->UserData = setup.UserData;

                engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                userData = setup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            internal bool ProcessIoUringCompletionSuccessAccept(int result)
            {
                AcceptedFileDescriptor = (IntPtr)result;
                ErrorCode = SocketError.Success;
                AcceptSocketAddressLength = SocketAddress.Length;
                return true;
            }

            internal bool ProcessIoUringCompletionErrorAccept(int result)
            {
                AcceptedFileDescriptor = (IntPtr)(-1);
                return ProcessIoUringCompletionErrorRead(result);
            }
        }

        private sealed partial class ConnectOperation
        {
            internal override Interop.Sys.SocketEvents GetIoUringFallbackSocketEvents() =>
                Interop.Sys.SocketEvents.Write;

            protected override unsafe IoUringDirectPrepareResult IoUringPrepareDirect(
                SocketAsyncContext context,
                SocketAsyncEngine engine,
                out ulong userData)
            {
                userData = 0;

                if (!TryPinIoUringBuffer(SocketAddress, out byte* rawSocketAddress))
                    return IoUringDirectPrepareResult.PrepareFailed;

                var setup = engine.TrySetupDirectSqe(context._socket, IoUringOpConnect);
                if (setup.PrepareResult != IoUringDirectPrepareResult.Prepared)
                {
                    ErrorCode = setup.ErrorCode;
                    return setup.PrepareResult;
                }

                SocketAsyncEngine.IoUringSqe* sqe = setup.Sqe;
                sqe->Opcode = IoUringOpConnect;
                sqe->Flags = setup.SqeFlags;
                sqe->Ioprio = 0;
                sqe->Fd = setup.SqeFd;
                sqe->Addr = (ulong)(nuint)rawSocketAddress;
                sqe->Off = (uint)SocketAddress.Length;
                sqe->Len = 0;
                sqe->RwFlags = 0;
                sqe->UserData = setup.UserData;

                engine.FinishSubmission(setup.SlotIndex, setup.UserData, this);
                userData = setup.UserData;
                ErrorCode = SocketError.Success;
                return IoUringDirectPrepareResult.Prepared;
            }

            internal bool ProcessIoUringCompletionErrorConnect(SocketAsyncContext context, int result)
            {
                Interop.Error error = GetIoUringPalError(result);
                if (error == Interop.Error.EINPROGRESS)
                {
                    ErrorCode = SocketError.Success;
                    return false;
                }

                if (!ProcessIoUringCompletionErrorWrite(result))
                    return false;

                context._socket.RegisterConnectResult(ErrorCode);
                return true;
            }

            internal bool ProcessIoUringCompletionSuccessConnect(SocketAsyncContext context)
            {
                ErrorCode = SocketError.Success;
                context._socket.RegisterConnectResult(ErrorCode);

                if (Buffer.Length > 0)
                {
                    Action<int, Memory<byte>, SocketFlags, SocketError>? callback = Callback;
                    Debug.Assert(callback is not null);
                    SocketError error = context.SendToAsync(Buffer, 0, Buffer.Length, SocketFlags.None, default, ref BytesTransferred, callback!, default);
                    if (error == SocketError.IOPending)
                    {
                        Callback = null;
                        Buffer = default;
                    }
                    else
                    {
                        if (error != SocketError.Success)
                        {
                            ErrorCode = error;
                            context._socket.RegisterConnectResult(ErrorCode);
                        }

                        Buffer = default;
                    }
                }

                return true;
            }
        }

        // ===================================================================
        // io_uring async dispatch implementations
        // ===================================================================

        private partial SocketError IoUringAcceptAsync(Memory<byte> socketAddress, out int socketAddressLen, out IntPtr acceptedFd, Action<IntPtr, Memory<byte>, SocketError> callback, CancellationToken cancellationToken)
        {
            int observedSequenceNumber = _receiveQueue.GetCurrentSequenceNumber();

            AcceptOperation operation = RentAcceptOperation();
            operation.Callback = callback;
            operation.SocketAddress = socketAddress;

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                socketAddressLen = operation.SocketAddress.Length;
                acceptedFd = operation.AcceptedFileDescriptor;
                SocketError errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            acceptedFd = (IntPtr)(-1);
            socketAddressLen = 0;
            return SocketError.IOPending;
        }

        private partial SocketError IoUringConnectAsync(Memory<byte> socketAddress, Action<int, Memory<byte>, SocketFlags, SocketError> callback, Memory<byte> buffer, out int sentBytes, CancellationToken cancellationToken)
        {
            SocketError errorCode;
            int observedSequenceNumber = _sendQueue.GetCurrentSequenceNumber();
            if (SocketPal.TryStartConnect(_socket, socketAddress, out errorCode, buffer.Span, false, out sentBytes))
            {
                _socket.RegisterConnectResult(errorCode);

                int remains = buffer.Length - sentBytes;
                if (errorCode == SocketError.Success && remains > 0)
                {
                    errorCode = SendToAsync(buffer.Slice(sentBytes), 0, remains, SocketFlags.None, Memory<byte>.Empty, ref sentBytes, callback!, default);
                }
                return errorCode;
            }

            var operation = new ConnectOperation(this)
            {
                Callback = callback,
                SocketAddress = socketAddress,
                Buffer = buffer.Slice(sentBytes),
                BytesTransferred = sentBytes,
            };

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                if (operation.ErrorCode == SocketError.Success)
                {
                    sentBytes += operation.BytesTransferred;
                }
                return operation.ErrorCode;
            }

            return SocketError.IOPending;
        }

        private partial SocketError IoUringReceiveAsync(Memory<byte> buffer, SocketFlags flags, out int bytesReceived, Action<int, Memory<byte>, SocketFlags, SocketError> callback, CancellationToken cancellationToken)
        {
            int observedSequenceNumber = _receiveQueue.GetCurrentSequenceNumber();

            BufferMemoryReceiveOperation operation = RentBufferMemoryReceiveOperation();
            operation.SetReceivedFlags = false;
            operation.Callback = callback;
            operation.Buffer = buffer;
            operation.Flags = flags;
            operation.SocketAddress = default;

            // Multishot recv fast path: arm on first recv, then check for pre-received data or register as pending.
            // Only for simple recv (no flags, connected socket).
            if (flags == SocketFlags.None && (_multishotRecvArmed || TryArmMultishotRecv()))
            {
                if (TryMultishotRecvFastPath(operation))
                {
                    if (operation.ErrorCode == SocketError.Success && operation.BytesTransferred > 0)
                    {
                        // Completed synchronously from queued data
                        bytesReceived = operation.BytesTransferred;
                        ReturnOperation(operation);
                        return SocketError.Success;
                    }
                    // Registered as pending — will be completed by multishot CQE
                    bytesReceived = 0;
                    return SocketError.IOPending;
                }
            }

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                bytesReceived = operation.BytesTransferred;
                SocketError errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            bytesReceived = 0;
            return SocketError.IOPending;
        }

        private partial SocketError IoUringReceiveFromAsync(Memory<byte> buffer, SocketFlags flags, Memory<byte> socketAddress, out int socketAddressLen, out int bytesReceived, out SocketFlags receivedFlags, Action<int, Memory<byte>, SocketFlags, SocketError> callback, CancellationToken cancellationToken)
        {
            int observedSequenceNumber = _receiveQueue.GetCurrentSequenceNumber();

            BufferMemoryReceiveOperation operation = RentBufferMemoryReceiveOperation();
            operation.SetReceivedFlags = true;
            operation.Callback = callback;
            operation.Buffer = buffer;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                receivedFlags = operation.ReceivedFlags;
                bytesReceived = operation.BytesTransferred;
                SocketError errorCode = operation.ErrorCode;
                socketAddressLen = operation.SocketAddress.Length;

                ReturnOperation(operation);
                return errorCode;
            }

            bytesReceived = 0;
            socketAddressLen = 0;
            receivedFlags = SocketFlags.None;
            return SocketError.IOPending;
        }

        private partial SocketError IoUringReceiveFromAsync(IList<ArraySegment<byte>> buffers, SocketFlags flags, Memory<byte> socketAddress, out int socketAddressLen, out int bytesReceived, out SocketFlags receivedFlags, Action<int, Memory<byte>, SocketFlags, SocketError> callback)
        {
            int observedSequenceNumber = _receiveQueue.GetCurrentSequenceNumber();

            BufferListReceiveOperation operation = RentBufferListReceiveOperation();
            operation.Callback = callback;
            operation.Buffers = buffers;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                socketAddressLen = operation.SocketAddress.Length;
                receivedFlags = operation.ReceivedFlags;
                bytesReceived = operation.BytesTransferred;
                SocketError errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            receivedFlags = SocketFlags.None;
            socketAddressLen = 0;
            bytesReceived = 0;
            return SocketError.IOPending;
        }

        private partial SocketError IoUringReceiveMessageFromAsync(Memory<byte> buffer, IList<ArraySegment<byte>>? buffers, SocketFlags flags, Memory<byte> socketAddress, out int socketAddressLen, bool isIPv4, bool isIPv6, out int bytesReceived, out SocketFlags receivedFlags, out IPPacketInformation ipPacketInformation, Action<int, Memory<byte>, SocketFlags, IPPacketInformation, SocketError> callback, CancellationToken cancellationToken)
        {
            int observedSequenceNumber = _receiveQueue.GetCurrentSequenceNumber();

            var operation = new ReceiveMessageFromOperation(this)
            {
                Callback = callback,
                Buffer = buffer,
                Buffers = buffers,
                Flags = flags,
                SocketAddress = socketAddress,
                IsIPv4 = isIPv4,
                IsIPv6 = isIPv6,
            };

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                socketAddressLen = operation.SocketAddress.Length;
                receivedFlags = operation.ReceivedFlags;
                ipPacketInformation = operation.IPPacketInformation;
                bytesReceived = operation.BytesTransferred;
                return operation.ErrorCode;
            }

            ipPacketInformation = default(IPPacketInformation);
            bytesReceived = 0;
            socketAddressLen = 0;
            receivedFlags = SocketFlags.None;
            return SocketError.IOPending;
        }

        private partial SocketError IoUringSendToAsync(Memory<byte> buffer, int offset, int count, SocketFlags flags, Memory<byte> socketAddress, ref int bytesSent, Action<int, Memory<byte>, SocketFlags, SocketError> callback, CancellationToken cancellationToken)
        {
            int observedSequenceNumber = _sendQueue.GetCurrentSequenceNumber();

            BufferMemorySendOperation operation = RentBufferMemorySendOperation();
            operation.Callback = callback;
            operation.Buffer = buffer;
            operation.Offset = offset;
            operation.Count = count;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;
            operation.BytesTransferred = bytesSent;

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                bytesSent = operation.BytesTransferred;
                SocketError errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            return SocketError.IOPending;
        }

        private partial SocketError IoUringSendToAsync(IList<ArraySegment<byte>> buffers, SocketFlags flags, Memory<byte> socketAddress, out int bytesSent, Action<int, Memory<byte>, SocketFlags, SocketError> callback)
        {
            bytesSent = 0;
            int observedSequenceNumber = _sendQueue.GetCurrentSequenceNumber();

            BufferListSendOperation operation = RentBufferListSendOperation();
            operation.Callback = callback;
            operation.Buffers = buffers;
            operation.BufferIndex = 0;
            operation.Offset = 0;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;
            operation.BytesTransferred = 0;

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                bytesSent = operation.BytesTransferred;
                SocketError errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            return SocketError.IOPending;
        }

        private partial SocketError IoUringSendFileAsync(SafeFileHandle fileHandle, long offset, long count, out long bytesSent, Action<long, SocketError> callback, CancellationToken cancellationToken)
        {
            bytesSent = 0;
            int observedSequenceNumber = _sendQueue.GetCurrentSequenceNumber();

            var operation = new SendFileOperation(this)
            {
                Callback = callback,
                FileHandle = fileHandle,
                Offset = offset,
                Count = count,
                BytesTransferred = bytesSent
            };

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                bytesSent = operation.BytesTransferred;
                return operation.ErrorCode;
            }

            return SocketError.IOPending;
        }

        // ===================================================================
        // OperationQueue extensions for io_uring
        // ===================================================================

        private partial struct OperationQueue<TOperation>
            where TOperation : AsyncOperation
        {
            /// <summary>Returns the current sequence number for io_uring callers that skip the IsReady check.</summary>
            public int GetCurrentSequenceNumber() => Volatile.Read(ref _sequenceNumber);

            public bool TryRemoveCompletedOperation(SocketAsyncContext context, TOperation operation)
            {
                using (Lock())
                {
                    if (_tail == null || _state == QueueState.Stopped)
                    {
                        return false;
                    }

                    AsyncOperation? previous = _tail;
                    AsyncOperation? current = _tail.Next;
                    while (!ReferenceEquals(current, operation))
                    {
                        if (ReferenceEquals(current, _tail))
                        {
                            return false;
                        }

                        previous = current;
                        current = current!.Next;
                    }

                    Debug.Assert(previous != null && current != null);
                    bool removedHead = ReferenceEquals(current, _tail.Next);
                    bool removedTail = ReferenceEquals(current, _tail);

                    if (removedHead && removedTail)
                    {
                        _tail = null;
                        _isNextOperationSynchronous = false;
                        _state = QueueState.Ready;
                        _sequenceNumber++;
                        Trace(context, $"Removed completed {IdOf(operation)} (queue empty)");
                        return true;
                    }

                    previous!.Next = current!.Next;
                    if (removedTail)
                    {
                        _tail = (TOperation)previous;
                    }

                    if (removedHead)
                    {
                        Debug.Assert(_tail != null);
                        _isNextOperationSynchronous = _tail.Next.Event != null;
                    }

                    Trace(context, $"Removed completed {IdOf(operation)}");
                    return true;
                }
            }
        }
    }
}
