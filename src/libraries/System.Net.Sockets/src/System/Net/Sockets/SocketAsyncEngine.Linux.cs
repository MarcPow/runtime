// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace System.Net.Sockets
{
    /// <summary>
    /// io_uring engine for Linux socket I/O.
    ///
    /// Architecture:
    ///   User thread:  lock(sqLock) { write SQE, bump tail } → io_uring_enter(submit=1) → IOPending
    ///   Kernel:       reads SQE → does I/O → writes CQE → signals eventfd
    ///   ThreadPool:   epoll wakes on eventfd → DrainCompletions() → dispatch callbacks
    ///
    /// No dedicated event loop thread. No MPSC queues. No EAGAIN retry paths.
    /// </summary>
    internal sealed unsafe partial class SocketAsyncEngine
    {
        // ---- Ring state ----
        private ManagedRingState _ringState = ManagedRingState.CreateDefault();
        private Interop.Sys.IoUringSqRingInfo _ioUringSqRingInfo;
        private bool _managedSqeInvariantsValidated;

        // ---- SQ submission ----
        private readonly Lock _sqLock = new Lock();
        private uint _sqTail;
        private bool _sqTailLoaded;

        // ---- Completion slots (user_data → operation mapping) ----
        // Defined in IoUringSlots.Linux.cs: AllocateCompletionSlot, FreeCompletionSlot
        private IoUringCompletionSlot[]? _completionSlots;
        private IoUringTrackedOperationState[]? _trackedOperations;
        private IoUringCompletionSlotStorage[]? _completionSlotStorage;
        private unsafe byte* _completionSlotNativeStorage;
        private nuint _completionSlotNativeStorageStride;
        private int _completionSlotFreeListHead = -1;
        private int _completionSlotsInUse;
        private int _completionSlotsHighWaterMark;
        private int _liveAcceptCompletionSlotCount;
        private int _trackedIoUringOperationCount;
        private int _ioUringSlotCapacity;

        // ---- Capabilities ----
        private LinuxIoUringCapabilities _ioUringCapabilities;
        private bool _ioUringInitialized;
        private bool _ioUringDirectSqeEnabled;

        // Per-opcode support
        private bool _supportsOpSend;
        private bool _supportsOpRecv;
        private bool _supportsOpSendMsg;
        private bool _supportsOpRecvMsg;
        private bool _supportsOpAccept;
        private bool _supportsOpConnect;
        private bool _supportsOpAsyncCancel;
        private bool _supportsMultishotAccept;

        // ---- Teardown ----
        private int _ioUringTeardownInitiated;

        // Engine topology (s_fdEngineAffinity, EngineCount, GetEngineByIndex, etc.) are in SocketAsyncEngine.Unix.cs

        /// <summary>Whether this engine uses io_uring completion mode.</summary>
        internal bool IsIoUringCompletionModeEnabled => _ioUringCapabilities.IsCompletionMode;

        /// <summary>Whether direct SQE submission is enabled.</summary>
        internal bool IsIoUringDirectSqeEnabled => _ioUringDirectSqeEnabled;

        // Config string constants used by IoUring.Configuration.cs
        private const string UseIoUringAppContextSwitch = "System.Net.Sockets.UseIoUring";
        private const string UseIoUringSqPollAppContextSwitch = "System.Net.Sockets.UseIoUringSqPoll";

        // ==================================================================
        // io_uring setup
        // ==================================================================

        /// <summary>
        /// Detects and initializes io_uring for this engine instance.
        /// Called once during engine construction on the event loop thread.
        /// </summary>
        partial void LinuxDetectAndInitializeIoUring()
        {
            if (!IsIoUringKernelVersionSupported())
                return;

            IoUringResolvedConfiguration resolvedConfiguration = ResolveIoUringConfiguration();
            if (!resolvedConfiguration.IoUringEnabled)
                return;

            if (!TryInitializeIoUringCompletionMode(resolvedConfiguration))
                return;

            _ioUringInitialized = true;
        }

        private bool TryInitializeIoUringCompletionMode(in IoUringResolvedConfiguration resolvedConfiguration)
        {
            bool sqPollRequested = resolvedConfiguration.SqPollRequested;
            if (!TrySetupIoUring(sqPollRequested, out IoUringSetupResult setupResult))
                return false;

            if (!TryMmapRings(ref setupResult))
                return false;

            // Probe opcode support
            ProbeIoUringOpcodeSupport(setupResult.RingFd);

            // Initialize completion slot pool
            int slotCapacity = (int)IoUringConstants.QueueEntries * IoUringConstants.CompletionOperationPoolCapacityFactor;
            _ioUringSlotCapacity = slotCapacity;
            InitializeCompletionSlotPool(slotCapacity);

            // Create eventfd for CQ notifications
            int eventFd = Interop.Sys.EventFd(0, Interop.Sys.EventFdFlags.EFD_CLOEXEC | Interop.Sys.EventFdFlags.EFD_NONBLOCK);
            if (eventFd < 0)
            {
                CleanupManagedRings();
                return false;
            }

            // Register eventfd with io_uring
            Interop.Error regError = Interop.Sys.IoUringShimRegisterEventfd(setupResult.RingFd, eventFd);
            if (regError != Interop.Error.SUCCESS)
            {
                Interop.Sys.Close((IntPtr)eventFd);
                CleanupManagedRings();
                return false;
            }

            _ringState.WakeupEventFd = eventFd;
            _ringState.RingFd = setupResult.RingFd;
            _ringState.UsesExtArg = setupResult.UsesExtArg;
            _ringState.NegotiatedFlags = setupResult.NegotiatedFlags;

            // Validate SQ ring invariants and enable direct SQE
            if (!ValidateManagedSqeInitializationInvariants())
            {
                Interop.Sys.Close((IntPtr)eventFd);
                CleanupManagedRings();
                return false;
            }
            _ioUringDirectSqeEnabled = true;

            // Set capabilities
            _ioUringCapabilities = default(LinuxIoUringCapabilities)
                .WithIsIoUringPort(true)
                .WithMode(IoUringMode.Completion)
                .WithSupportsMultishotAccept(_supportsMultishotAccept);

            // Register the eventfd with the epoll-based event loop so ThreadPool
            // workers wake up when CQEs arrive.
            RegisterEventFdWithEpoll(eventFd);

            return true;
        }

        /// <summary>
        /// Registers the io_uring eventfd with the engine's epoll port so that
        /// the existing ThreadPool dispatch (HandleSocketEvents) wakes when CQEs arrive.
        /// </summary>
        private void RegisterEventFdWithEpoll(int eventFd)
        {
            // The epoll-based event loop already handles fd events via the native shim.
            // Register the eventfd as a read-ready source. When the kernel signals the
            // eventfd (CQE produced), epoll fires, and the ThreadPool worker drains CQEs.
            Interop.Error error = Interop.Sys.TryChangeSocketEventRegistration(
                _port, (IntPtr)eventFd,
                Interop.Sys.SocketEvents.None,
                Interop.Sys.SocketEvents.Read,
                (int)IoUringConstants.TagWakeupSignal);

            if (error != Interop.Error.SUCCESS)
            {
                // Non-fatal: io_uring will still work, just won't get epoll-driven CQ drain.
                // The event loop's periodic wake will drain CQEs eventually.
            }
        }

        // ==================================================================
        // SQE submission (from any thread)
        // ==================================================================

        /// <summary>
        /// Allocates a completion slot and SQE under lock.
        /// Returns the setup info for the caller to write SQE fields.
        /// Lock is held on return if Prepared — caller must call FinishSubmission().
        /// </summary>
        internal IoUringDirectSqeSetupResult TrySetupDirectSqe(
            SafeSocketHandle socket,
            byte opcode)
        {
            IoUringDirectSqeSetupResult setup = default;
            setup.SlotIndex = -1;
            setup.PrepareResult = SocketAsyncContext.AsyncOperation.IoUringDirectPrepareResult.Unsupported;
            setup.ErrorCode = SocketError.Success;

            if (!_ioUringDirectSqeEnabled)
                return setup;

            _sqLock.Enter();

            int slotIndex = AllocateCompletionSlot();
            if (slotIndex < 0)
            {
                _sqLock.Exit();
                return setup;
            }

            setup.SlotIndex = slotIndex;
            ref IoUringCompletionSlot slot = ref _completionSlots![slotIndex];
            ref IoUringCompletionSlotStorage slotStorage = ref _completionSlotStorage![slotIndex];
            setup.UserData = EncodeCompletionSlotUserData(slotIndex, slot.Generation);

            bool addedRef = false;
            try { socket.DangerousAddRef(ref addedRef); }
            catch (ObjectDisposedException)
            {
                FreeCompletionSlotUnderLock(slotIndex);
                _sqLock.Exit();
                setup.ErrorCode = SocketError.OperationAborted;
                setup.PrepareResult = SocketAsyncContext.AsyncOperation.IoUringDirectPrepareResult.PrepareFailed;
                return setup;
            }

            if (!addedRef)
            {
                FreeCompletionSlotUnderLock(slotIndex);
                _sqLock.Exit();
                setup.ErrorCode = SocketError.OperationAborted;
                setup.PrepareResult = SocketAsyncContext.AsyncOperation.IoUringDirectPrepareResult.PrepareFailed;
                return setup;
            }

            slotStorage.DangerousRefSocketHandle = socket;
            int socketFd = (int)(nint)socket.DangerousGetHandle();
            setup.SqeFd = socketFd;
            setup.SqeFlags = 0;

            if (!TryGetNextSqe(out IoUringSqe* sqe))
            {
                slotStorage.DangerousRefSocketHandle = null;
                socket.DangerousRelease();
                FreeCompletionSlotUnderLock(slotIndex);
                _sqLock.Exit();
                return setup;
            }

            // Lock stays held — caller writes SQE fields, then calls FinishSubmission
            setup.Sqe = sqe;
            setup.PrepareResult = SocketAsyncContext.AsyncOperation.IoUringDirectPrepareResult.Prepared;
            return setup;
        }

        /// <summary>
        /// Publishes the SQ tail, tracks the operation, releases lock, and submits to kernel.
        /// </summary>
        internal void FinishSubmission(int slotIndex, ulong userData, SocketAsyncContext.AsyncOperation operation)
        {
            // Track operation under lock (before kernel can see the SQE)
            ref IoUringTrackedOperationState entry = ref _trackedOperations![slotIndex];
            ulong generation = (userData >> IoUringConstants.SlotIndexBits) & IoUringConstants.GenerationMask;
            Volatile.Write(ref entry.TrackedOperationGeneration, generation);
            Volatile.Write(ref entry.TrackedOperation, operation);
            Interlocked.Increment(ref _trackedIoUringOperationCount);

            // Publish SQ tail to kernel
            PublishSqTail();
            _sqLock.Exit();

            // Submit to kernel (outside lock)
            IoUringEnter(1, 0);
        }

        /// <summary>Aborts a prepared SQE — releases socket ref, frees slot, undoes SQ bump.</summary>
        internal void AbortSubmission(int slotIndex, SafeSocketHandle socket)
        {
            ref IoUringCompletionSlotStorage slotStorage = ref _completionSlotStorage![slotIndex];
            slotStorage.DangerousRefSocketHandle = null;
            socket.DangerousRelease();
            _sqTail--;
            FreeCompletionSlotUnderLock(slotIndex);
            _sqLock.Exit();
        }

        /// <summary>Gets the next SQE slot from the ring. Must hold _sqLock.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryGetNextSqe(out IoUringSqe* sqe)
        {
            sqe = null;
            ref Interop.Sys.IoUringSqRingInfo ringInfo = ref _ioUringSqRingInfo;

            uint sqHead = Volatile.Read(ref *(uint*)ringInfo.SqHeadPtr);
            if (!_sqTailLoaded)
            {
                _sqTail = Volatile.Read(ref *(uint*)ringInfo.SqTailPtr);
                _sqTailLoaded = true;
            }

            if (_sqTail - sqHead >= ringInfo.SqEntries)
                return false; // SQ ring full

            uint index = _sqTail & ringInfo.SqMask;
            nint sqeOffset = checked((nint)((nuint)index * ringInfo.SqeSize));
            sqe = (IoUringSqe*)((byte*)ringInfo.SqeBase + sqeOffset);
            Unsafe.InitBlockUnaligned((byte*)sqe + 40, 0, 24);
            _sqTail++;
            return true;
        }

        /// <summary>Publishes SQ tail to kernel. Must hold _sqLock.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PublishSqTail()
        {
            if (_sqTailLoaded && _ioUringSqRingInfo.SqTailPtr != IntPtr.Zero)
            {
                ref uint sqTailRef = ref Unsafe.AsRef<uint>((void*)_ioUringSqRingInfo.SqTailPtr);
                Volatile.Write(ref sqTailRef, _sqTail);
                _sqTailLoaded = false;
            }
        }

        /// <summary>Calls io_uring_enter.</summary>
        private void IoUringEnter(uint toSubmit, uint minComplete)
        {
            uint flags = minComplete > 0 ? IoUringConstants.EnterGetevents : 0u;
            int ringFd = _ringState.RingFd;
            Interop.Sys.IoUringShimEnter(ringFd, toSubmit, minComplete, flags, null);
        }

        // ==================================================================
        // CQ drain (called from ThreadPool via epoll eventfd notification)
        // ==================================================================

        /// <summary>
        /// Drains all available CQEs from the CQ ring and dispatches completions.
        /// Called when the eventfd signals that CQEs are ready.
        /// </summary>
        internal void DrainCompletions()
        {
            // Consume the eventfd counter
            if (_ringState.WakeupEventFd >= 0)
            {
                ulong val;
                Interop.Sys.Read((IntPtr)_ringState.WakeupEventFd, (byte*)&val, 8);
            }

            // Drain CQ ring
            while (true)
            {
                uint head = Volatile.Read(ref *_ringState.CqHeadPtr);
                uint tail = Volatile.Read(ref *_ringState.CqTailPtr);
                if (head == tail) break;

                Interop.Sys.IoUringCqe* cqe = _ringState.CqeBase + (head & _ringState.CqMask);
                ulong userData = cqe->UserData;
                int result = cqe->Result;
                uint flags = cqe->Flags;
                Volatile.Write(ref *_ringState.CqHeadPtr, head + 1);

                // Dispatch the completion
                DispatchCompletion(userData, result, flags);
            }
        }

        /// <summary>Routes a single CQE to its tracked operation.</summary>
        private void DispatchCompletion(ulong userData, int result, uint flags)
        {
            if (userData == 0) return;

            byte tag = (byte)(userData >> 56);
            if (tag != IoUringConstants.TagReservedCompletion) return;

            ulong payload = userData & 0x00FF_FFFF_FFFF_FFFFUL;
            int slotIndex = (int)(payload & IoUringConstants.SlotIndexMask);
            ulong generation = (payload >> IoUringConstants.SlotIndexBits) & IoUringConstants.GenerationMask;

            if (_completionSlots is null || (uint)slotIndex >= (uint)_completionSlots.Length)
                return;

            ref IoUringCompletionSlot slot = ref _completionSlots[slotIndex];
            if (slot.Generation != generation)
                return; // Stale CQE

            // Take the tracked operation
            ref IoUringTrackedOperationState entry = ref _trackedOperations![slotIndex];
            SocketAsyncContext.AsyncOperation? operation = Interlocked.Exchange(ref entry.TrackedOperation, null);
            if (operation is null)
                return;

            Volatile.Write(ref entry.TrackedOperationGeneration, 0UL);
            Interlocked.Decrement(ref _trackedIoUringOperationCount);

            // Free the slot
            FreeCompletionSlot(slotIndex);

            // Process the result and dispatch callback
            var completionResult = operation.ProcessIoUringCompletionResult(result, flags, 0);
            switch (completionResult)
            {
                case SocketAsyncContext.AsyncOperation.IoUringCompletionResult.Completed:
                    operation.ClearIoUringUserData();
                    operation.AssociatedContext.TryCompleteIoUringOperation(operation);
                    break;

                case SocketAsyncContext.AsyncOperation.IoUringCompletionResult.Pending:
                    // Partial send/recv — re-submit via io_uring
                    operation.ClearIoUringUserData();
                    operation.TryQueueIoUringPreparation();
                    break;

                case SocketAsyncContext.AsyncOperation.IoUringCompletionResult.Canceled:
                case SocketAsyncContext.AsyncOperation.IoUringCompletionResult.Ignored:
                    operation.ClearIoUringUserData();
                    break;
            }
        }

        // ==================================================================
        // io_uring setup helpers
        // ==================================================================

        /// <summary>Calls io_uring_setup and negotiates feature flags.</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static bool TrySetupIoUring(bool sqPollRequested, out IoUringSetupResult setupResult)
        {
            setupResult = default;
            uint queueEntries = IoUringConstants.QueueEntries;

            // Multi-thread submission: any thread can write SQEs and call io_uring_enter.
            uint flags = IoUringConstants.SetupCqSize | IoUringConstants.SetupSubmitAll
                       | IoUringConstants.SetupCoopTaskrun
                       | IoUringConstants.SetupNoSqArray | IoUringConstants.SetupCloexec;

            if (sqPollRequested)
                flags |= IoUringConstants.SetupSqPoll;

            // Peel unsupported flags
            ReadOnlySpan<uint> flagsToPeel = [IoUringConstants.SetupNoSqArray, IoUringConstants.SetupCloexec];

            Interop.Sys.IoUringParams ioParams;
            int ringFd;
            Interop.Error err;
            int peelIndex = 0;
            while (true)
            {
                ioParams = default;
                ioParams.Flags = flags;
                ioParams.CqEntries = queueEntries * IoUringConstants.CqEntriesFactor;
                err = Interop.Sys.IoUringShimSetup(queueEntries, &ioParams, &ringFd);

                if (err != Interop.Error.EINVAL)
                    break;

                while (peelIndex < flagsToPeel.Length && (flags & flagsToPeel[peelIndex]) == 0)
                    peelIndex++;

                if (peelIndex >= flagsToPeel.Length)
                    break;

                flags &= ~flagsToPeel[peelIndex++];
            }

            if (err != Interop.Error.SUCCESS)
                return false;

            if (!TrySetFdCloseOnExec(ringFd))
            {
                Interop.Sys.IoUringShimCloseFd(ringFd);
                return false;
            }

            setupResult.RingFd = ringFd;
            setupResult.Params = ioParams;
            setupResult.NegotiatedFlags = flags;
            setupResult.UsesExtArg = (ioParams.Features & IoUringConstants.FeatureExtArg) != 0;
            return true;
        }

        private static bool TrySetFdCloseOnExec(int fd)
        {
            return Interop.Sys.Fcntl.SetFD((IntPtr)fd, 1) == 0; // FD_CLOEXEC
        }

        private static bool IsIoUringKernelVersionSupported()
        {
            // Linux 6.1+ for stable SEND_ZC and completion semantics
            try
            {
                var version = Environment.OSVersion.Version;
                return version.Major > IoUringConstants.MinKernelMajor ||
                       (version.Major == IoUringConstants.MinKernelMajor &&
                        version.Minor >= IoUringConstants.MinKernelMinor);
            }
            catch
            {
                return false;
            }
        }

        private void ProbeIoUringOpcodeSupport(int ringFd)
        {
            // Probe kernel for supported opcodes
            const int probeSize = 256;
            byte* probeBuffer = stackalloc byte[16 + probeSize * 8];
            Interop.Error probeError = Interop.Sys.IoUringShimRegister(
                ringFd, IoUringConstants.RegisterProbe, probeBuffer, (uint)probeSize);

            if (probeError != Interop.Error.SUCCESS)
                return;

            IoUringProbeHeader* header = (IoUringProbeHeader*)probeBuffer;
            IoUringProbeOp* ops = (IoUringProbeOp*)(probeBuffer + 16);
            int opsCount = header->OpsLen;

            _supportsOpSend = IsOpcodeSupported(ops, opsCount, IoUringOpcodes.Send);
            _supportsOpRecv = IsOpcodeSupported(ops, opsCount, IoUringOpcodes.Recv);
            _supportsOpSendMsg = IsOpcodeSupported(ops, opsCount, IoUringOpcodes.SendMsg);
            _supportsOpRecvMsg = IsOpcodeSupported(ops, opsCount, IoUringOpcodes.RecvMsg);
            _supportsOpAccept = IsOpcodeSupported(ops, opsCount, IoUringOpcodes.Accept);
            _supportsOpConnect = IsOpcodeSupported(ops, opsCount, IoUringOpcodes.Connect);
            _supportsOpAsyncCancel = IsOpcodeSupported(ops, opsCount, IoUringOpcodes.AsyncCancel);
            _supportsMultishotAccept = _supportsOpAccept;
        }

        private static bool IsOpcodeSupported(IoUringProbeOp* ops, int opsCount, byte opcode)
        {
            if (opcode >= opsCount) return false;
            return (ops[opcode].Flags & IoUringConstants.ProbeOpFlagSupported) != 0;
        }

        private bool ValidateManagedSqeInitializationInvariants()
        {
            ref Interop.Sys.IoUringSqRingInfo ringInfo = ref _ioUringSqRingInfo;
            if (ringInfo.SqeBase == IntPtr.Zero ||
                ringInfo.SqHeadPtr == IntPtr.Zero ||
                ringInfo.SqTailPtr == IntPtr.Zero ||
                ringInfo.SqEntries == 0)
                return false;

            if (ringInfo.SqeSize != (uint)sizeof(IoUringSqe))
                return false;

            _managedSqeInvariantsValidated = true;
            return true;
        }

        // ==================================================================
        // Teardown
        // ==================================================================

        partial void LinuxBeforeFreeNativeResources(ref bool closeSocketEventPort)
        {
            if (!_ioUringCapabilities.IsIoUringPort || _port == (IntPtr)(-1))
                return;

            Volatile.Write(ref _ioUringTeardownInitiated, 1);

            Interop.Error closeError = Interop.Sys.CloseSocketEventPort(_port);
            if (closeError == Interop.Error.SUCCESS)
                closeSocketEventPort = false;
        }

        // LinuxFreeIoUringResources is in IoUring.Rings.cs

        // ==================================================================
        // Helpers used by other partial files
        // ==================================================================

        internal static ulong EncodeIoUringUserData(byte tag, ulong payload)
            => ((ulong)tag << 56) | (payload & 0x00FF_FFFF_FFFF_FFFFUL);

        // IsMultishotAcceptDisabled and IsReusePortAcceptDisabled are in IoUring.Configuration.cs

        /// <summary>Decode completion slot index from user_data payload.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int DecodeDirectSqeSlotIndex(ulong userData)
        {
            ulong payload = userData & 0x00FF_FFFF_FFFF_FFFFUL;
            return (int)(payload & IoUringConstants.SlotIndexMask);
        }

        /// <summary>Struct holding direct SQE setup results.</summary>
        internal struct IoUringDirectSqeSetupResult
        {
            public SocketAsyncContext.AsyncOperation.IoUringDirectPrepareResult PrepareResult;
            public int SlotIndex;
            public ulong UserData;
            public int SqeFd;
            public byte SqeFlags;
            public IoUringSqe* Sqe;
            public SocketError ErrorCode;
        }
    }
}
