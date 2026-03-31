// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace System.Net.Sockets
{
    /// <summary>
    /// Provided buffer ring and multishot recv support.
    ///
    /// Multishot recv submits ONE recv SQE per socket (with IORING_RECV_MULTISHOT).
    /// The kernel delivers a CQE with CQE_F_MORE every time data arrives, drawing
    /// a buffer from the provided buffer ring. This eliminates the per-recv
    /// io_uring_enter syscall after the initial arm.
    /// </summary>
    internal sealed unsafe partial class SocketAsyncEngine
    {
        // ---- Provided buffer ring state ----
        private byte* _providedBufRingAddr;   // Ring of io_uring_buf entries
        private byte* _providedBufMemory;     // Contiguous buffer pool
        private int _providedBufSize;         // Bytes per buffer
        private int _providedBufCount;        // Total buffers (power of 2)
        private uint _providedBufRingMask;
        private uint _providedBufRingTail;    // Next slot to write when recycling
        private bool _providedBufferRingEnabled;

        internal const ushort ProvidedBufGroupId = 0;
        private const int ProvidedBufDefaultSize = 4096;   // Enough for HTTP requests
        private const int ProvidedBufDefaultCount = 2048;  // Must be power of 2

        /// <summary>Whether multishot recv with provided buffers is available.</summary>
        internal bool IsMultishotRecvEnabled => _providedBufferRingEnabled;

        // ==================================================================
        // Provided buffer ring setup
        // ==================================================================

        /// <summary>
        /// Allocates and registers a provided buffer ring with io_uring.
        /// Called once during engine initialization. Non-fatal on failure.
        /// </summary>
        private void TrySetupProvidedBufferRing(int ringFd)
        {
            IoUringDiag($"TrySetupProvidedBufferRing: ringFd={ringFd}");
            int bufCount = ProvidedBufDefaultCount;
            int bufSize = ProvidedBufDefaultSize;

            // Allocate the ring structure (array of IoUringBuf entries).
            // Must be contiguous; entry 0's tail field (offset 14) is the ring tail.
            int ringBytes = bufCount * sizeof(IoUringBuf);
            byte* ringAddr = (byte*)NativeMemory.AlignedAlloc((nuint)ringBytes, 4096);
            if (ringAddr == null) return;
            NativeMemory.Clear(ringAddr, (nuint)ringBytes);

            // Allocate contiguous buffer memory
            byte* bufMemory = (byte*)NativeMemory.AllocZeroed((nuint)(bufCount * bufSize));
            if (bufMemory == null)
            {
                NativeMemory.AlignedFree(ringAddr);
                return;
            }

            // Fill ring entries — each buffer is available to the kernel
            uint mask = (uint)(bufCount - 1);
            for (int i = 0; i < bufCount; i++)
            {
                IoUringBuf* entry = (IoUringBuf*)(ringAddr + (i & mask) * sizeof(IoUringBuf));
                entry->Addr = (ulong)(bufMemory + i * bufSize);
                entry->Len = (uint)bufSize;
                entry->Bid = (ushort)i;
            }

            // Publish tail: all buffers available
            *(ushort*)(ringAddr + 14) = (ushort)bufCount;

            // Register with kernel via IORING_REGISTER_PBUF_RING
            IoUringBufReg reg = default;
            reg.RingAddr = (ulong)(nuint)ringAddr;
            reg.RingEntries = (uint)bufCount;
            reg.Bgid = ProvidedBufGroupId;
            reg.Flags = 0;

            int regResult;
            Interop.Error err = Interop.Sys.IoUringShimRegister(
                ringFd, IoUringConstants.RegisterPbufRing, &reg, 1, &regResult);

            if (err != Interop.Error.SUCCESS)
            {
                IoUringDiag($"Provided buffer ring registration FAILED: {err}, regResult={regResult}");
                NativeMemory.AlignedFree(ringAddr);
                NativeMemory.Free(bufMemory);
                return;
            }

            _providedBufRingAddr = ringAddr;
            _providedBufMemory = bufMemory;
            _providedBufSize = bufSize;
            _providedBufCount = bufCount;
            _providedBufRingMask = mask;
            _providedBufRingTail = (uint)bufCount;
            _providedBufferRingEnabled = true;
            IoUringDiag($"Provided buffer ring: {bufCount} x {bufSize}B = {bufCount * bufSize / 1024}KB, group={ProvidedBufGroupId}");
        }

        // ==================================================================
        // Buffer access and recycling
        // ==================================================================

        /// <summary>Gets a pointer to the provided buffer with the given ID.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetProvidedBuffer(int bufferId)
        {
            return _providedBufMemory + bufferId * _providedBufSize;
        }

        /// <summary>Returns the provided buffer size.</summary>
        internal int GetProvidedBufferSize() => _providedBufSize;

        /// <summary>
        /// Returns a consumed buffer to the provided buffer ring so the kernel can reuse it.
        /// Must be called after the data has been copied out of the buffer.
        /// </summary>
        internal void RecycleProvidedBuffer(int bufferId)
        {
            uint tail = _providedBufRingTail;
            uint index = tail & _providedBufRingMask;

            IoUringBuf* entry = (IoUringBuf*)(_providedBufRingAddr + index * (uint)sizeof(IoUringBuf));
            entry->Addr = (ulong)(_providedBufMemory + bufferId * _providedBufSize);
            entry->Len = (uint)_providedBufSize;
            entry->Bid = (ushort)bufferId;

            _providedBufRingTail = tail + 1;

            // Publish tail to kernel (u16 at offset 14 of the ring)
            Volatile.Write(ref *(ushort*)(_providedBufRingAddr + 14), (ushort)_providedBufRingTail);
        }

        // ==================================================================
        // Multishot recv SQE submission
        // ==================================================================

        /// <summary>
        /// Arms a multishot recv on the given socket. One SQE → many CQEs.
        /// Returns the completion slot index, or -1 on failure.
        /// </summary>
        internal int TryArmMultishotRecv(
            SafeSocketHandle socket,
            SocketAsyncContext context, // Reserved for per-socket tracking
            out ulong userData)
        {
            _ = context;
            userData = 0;
            if (!_providedBufferRingEnabled || !_ioUringDirectSqeEnabled)
                return -1;

            _sqLock.Enter();

            int slotIndex = AllocateCompletionSlot();
            if (slotIndex < 0)
            {
                _sqLock.Exit();
                return -1;
            }

            ref IoUringCompletionSlot slot = ref _completionSlots![slotIndex];
            userData = EncodeCompletionSlotUserData(slotIndex, slot.Generation);

            bool addedRef = false;
            try { socket.DangerousAddRef(ref addedRef); }
            catch (ObjectDisposedException)
            {
                FreeCompletionSlotUnderLock(slotIndex);
                _sqLock.Exit();
                return -1;
            }

            if (!addedRef)
            {
                FreeCompletionSlotUnderLock(slotIndex);
                _sqLock.Exit();
                return -1;
            }

            ref IoUringCompletionSlotStorage slotStorage = ref _completionSlotStorage![slotIndex];
            slotStorage.DangerousRefSocketHandle = socket;
            slotStorage.IsMultishot = true;

            int socketFd = (int)(nint)socket.DangerousGetHandle();

            if (!TryGetNextSqe(out IoUringSqe* sqe))
            {
                slotStorage.DangerousRefSocketHandle = null;
                slotStorage.IsMultishot = false;
                socket.DangerousRelease();
                FreeCompletionSlotUnderLock(slotIndex);
                _sqLock.Exit();
                return -1;
            }

            // Write multishot recv SQE with provided buffer selection
            WriteProvidedBufferRecvSqe(
                sqe,
                socketFd,
                sqeFlags: 0,
                userData,
                requestedLength: (uint)_providedBufSize,
                rwFlags: 0,
                bufferGroupId: ProvidedBufGroupId,
                ioprio: IoUringConstants.RecvMultishot);

            // Track: for multishot, we store the context (not a specific operation) via a sentinel.
            // The actual pending operation is managed by SocketAsyncContext.
            ref IoUringTrackedOperationState entry = ref _trackedOperations![slotIndex];
            ulong generation = (userData >> IoUringConstants.SlotIndexBits) & IoUringConstants.GenerationMask;
            Volatile.Write(ref entry.TrackedOperationGeneration, generation);
            // TrackedOperation stays null for multishot — context manages pending ops
            Interlocked.Increment(ref _trackedIoUringOperationCount);

            PublishSqTail();
            _sqLock.Exit();

            IoUringEnter(IoUringConstants.QueueEntries, 0);
            return slotIndex;
        }

        // ==================================================================
        // Multishot CQE dispatch
        // ==================================================================

        /// <summary>
        /// Handles a multishot CQE (CQE_F_MORE is set). The slot stays alive.
        /// Extracts buffer ID and routes to the socket context.
        /// </summary>
        private void DispatchMultishotCompletion(int slotIndex, int result, uint flags)
        {
            ref IoUringCompletionSlotStorage slotStorage = ref _completionSlotStorage![slotIndex];
            SafeSocketHandle? socketHandle = slotStorage.DangerousRefSocketHandle;
            if (socketHandle is null) return;

            // Extract provided buffer ID from CQE flags
            int bufferId = -1;
            if ((flags & IoUringConstants.CqeFBuffer) != 0)
                bufferId = (int)(flags >> IoUringConstants.CqeBufferShift);

            // Route to the socket's async context for multishot recv handling
            SocketAsyncContext? context = socketHandle.AsyncContext;
            context?.HandleMultishotRecvCompletion(result, bufferId, this);
        }

        /// <summary>
        /// Handles the terminal CQE of a multishot (no CQE_F_MORE).
        /// The kernel has disarmed the multishot. Clean up.
        /// </summary>
        private void DispatchMultishotTerminal(int slotIndex, SocketAsyncContext.AsyncOperation? operation, int result, uint flags)
        {
            _ = operation; _ = flags; // Terminal CQE — operation was already taken by caller
            ref IoUringCompletionSlotStorage slotStorage = ref _completionSlotStorage![slotIndex];
            SafeSocketHandle? socketHandle = slotStorage.DangerousRefSocketHandle;

            // Notify context that multishot is disarmed
            SocketAsyncContext? context = socketHandle?.AsyncContext;
            context?.HandleMultishotRecvTerminal(result, this);

            // Release socket ref (slot is freed by caller)
            if (socketHandle is not null)
            {
                socketHandle.DangerousRelease();
                slotStorage.DangerousRefSocketHandle = null;
            }
        }

        // ==================================================================
        // Structs for provided buffer ring kernel ABI
        // ==================================================================

        /// <summary>Diagnostic logging for io_uring multishot. Writes to stderr.</summary>
        private static volatile int s_diagInitialized;
        private static System.IO.StreamWriter? s_diagWriter;

        internal static void IoUringDiag(string message)
        {
            try
            {
                if (s_diagInitialized == 0 && Interlocked.CompareExchange(ref s_diagInitialized, 1, 0) == 0)
                {
                    s_diagWriter = new System.IO.StreamWriter(
                        new System.IO.FileStream("/tmp/iouring-diag.log",
                            System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))
                    { AutoFlush = true };
                }
                s_diagWriter?.WriteLine($"[{Environment.TickCount64}] {message}");
            }
            catch { }
        }

        /// <summary>Matches kernel struct io_uring_buf (16 bytes).</summary>
        [StructLayout(LayoutKind.Sequential)]
        private struct IoUringBuf
        {
            public ulong Addr;
            public uint Len;
            public ushort Bid;
            public ushort Resv;
        }

        /// <summary>Matches kernel struct io_uring_buf_reg (40 bytes).</summary>
        [StructLayout(LayoutKind.Sequential)]
        private struct IoUringBufReg
        {
            public ulong RingAddr;
            public uint RingEntries;
            public ushort Bgid;
            public ushort Flags;
            public ulong Resv0;
            public ulong Resv1;
            public ulong Resv2;
        }
    }
}
