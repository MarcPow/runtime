// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace System.Net.Sockets
{
    internal sealed unsafe partial class SocketAsyncEngine
    {
        /// <summary>Indicates which io_uring dispatch mode is active for this engine instance.</summary>
        private enum IoUringMode : byte
        {
            Disabled = 0,
            Completion = 1
        }

        /// <summary>Distinguishes cancellation requests issued during normal runtime from those during engine teardown.</summary>
        private enum IoUringCancellationOrigin : byte
        {
            Runtime = 0,
            Teardown = 1
        }

        /// <summary>Identifies which CQ-overflow recovery branch is active for logging/telemetry correlation.</summary>
        private enum IoUringCqOverflowRecoveryBranch : byte
        {
            MultishotAcceptArming = 0,
            Teardown = 1,
            // Steady-state branch: normal runtime overflow recovery outside teardown/accept-arm handoff.
            DualWave = 2
        }

        /// <summary>Tracks the lifecycle of an io_uring operation for debug assertions on valid state transitions.</summary>
        private enum IoUringOperationLifecycleState : byte
        {
            Queued = 0,
            Prepared = 1,
            Submitted = 2,
            Completed = 3,
            Canceled = 4,
            Detached = 5
        }

        /// <summary>Precomputed default receive strategy derived from immutable io_uring capabilities.</summary>
        private enum IoUringRecvStrategy : byte
        {
            FixedRecv = 0,
            MultishotProvidedBuffer = 1,
            OneshotProvidedBuffer = 2,
            PlainUserBuffer = 3
        }

        /// <summary>Result of attempting to remove a tracked operation by user_data.</summary>
        private enum IoUringTrackedOperationRemoveResult : byte
        {
            Removed = 0,
            NotFound = 1,
            Mismatch = 2
        }

        private enum IoUringCancellationEnqueueResult : byte
        {
            Failed = 0,
            Enqueued = 1,
            EnqueuedAndWoke = 2
        }

        /// <summary>Immutable snapshot of negotiated io_uring capabilities for this engine instance.</summary>
        private readonly struct LinuxIoUringCapabilities
        {
            private const uint FlagIsIoUringPort = 1u << 0;
            private const uint FlagSupportsMultishotRecv = 1u << 1;
            private const uint FlagSupportsMultishotAccept = 1u << 2;
            private const uint FlagSupportsZeroCopySend = 1u << 3;
            private const uint FlagSqPollEnabled = 1u << 4;
            private const uint FlagSupportsProvidedBufferRings = 1u << 5;
            private const uint FlagHasRegisteredBuffers = 1u << 6;

            private readonly uint _flags;

            /// <summary>The active io_uring dispatch mode.</summary>
            internal IoUringMode Mode { get; }

            /// <summary>Whether the engine's port was created as an io_uring instance.</summary>
            internal bool IsIoUringPort => (_flags & FlagIsIoUringPort) != 0;
            /// <summary>Whether multishot recv can be used by this engine instance.</summary>
            internal bool SupportsMultishotRecv => (_flags & FlagSupportsMultishotRecv) != 0;
            /// <summary>Whether multishot accept can be used by this engine instance.</summary>
            internal bool SupportsMultishotAccept => (_flags & FlagSupportsMultishotAccept) != 0;
            /// <summary>Whether zero-copy send is enabled for this engine instance.</summary>
            internal bool SupportsZeroCopySend => (_flags & FlagSupportsZeroCopySend) != 0;
            /// <summary>Whether SQPOLL mode is enabled for this engine instance.</summary>
            internal bool SqPollEnabled => (_flags & FlagSqPollEnabled) != 0;
            /// <summary>Whether provided-buffer rings are active for this engine instance.</summary>
            internal bool SupportsProvidedBufferRings => (_flags & FlagSupportsProvidedBufferRings) != 0;
            /// <summary>Whether provided buffers are currently registered with the kernel.</summary>
            internal bool HasRegisteredBuffers => (_flags & FlagHasRegisteredBuffers) != 0;

            /// <summary>Whether the engine is operating in full completion mode.</summary>
            internal bool IsCompletionMode =>
                Mode == IoUringMode.Completion;

            private LinuxIoUringCapabilities(IoUringMode mode, uint flags)
            {
                Mode = mode;
                _flags = flags;
            }

            internal LinuxIoUringCapabilities WithMode(IoUringMode mode) =>
                new LinuxIoUringCapabilities(mode, _flags);

            internal LinuxIoUringCapabilities WithIsIoUringPort(bool value) =>
                WithFlag(FlagIsIoUringPort, value);

            internal LinuxIoUringCapabilities WithSupportsMultishotRecv(bool value) =>
                WithFlag(FlagSupportsMultishotRecv, value);

            internal LinuxIoUringCapabilities WithSupportsMultishotAccept(bool value) =>
                WithFlag(FlagSupportsMultishotAccept, value);

            internal LinuxIoUringCapabilities WithSupportsZeroCopySend(bool value) =>
                WithFlag(FlagSupportsZeroCopySend, value);

            internal LinuxIoUringCapabilities WithSqPollEnabled(bool value) =>
                WithFlag(FlagSqPollEnabled, value);

            internal LinuxIoUringCapabilities WithSupportsProvidedBufferRings(bool value) =>
                WithFlag(FlagSupportsProvidedBufferRings, value);

            internal LinuxIoUringCapabilities WithHasRegisteredBuffers(bool value) =>
                WithFlag(FlagHasRegisteredBuffers, value);

            private LinuxIoUringCapabilities WithFlag(uint flag, bool value)
            {
                uint flags = value ? (_flags | flag) : (_flags & ~flag);
                return new LinuxIoUringCapabilities(Mode, flags);
            }
        }

        [Flags]
        private enum IoUringConfigurationWarningFlags : byte
        {
            None = 0,
            SqPollRequestedWithoutIoUring = 1 << 0,
            DirectSqeDisabledWithoutIoUring = 1 << 1,
            ZeroCopyOptInWithoutIoUring = 1 << 2
        }

        /// <summary>Immutable process-wide snapshot of resolved io_uring configuration inputs.</summary>
        private readonly struct IoUringResolvedConfiguration
        {
            internal bool IoUringEnabled { get; }
            internal bool SqPollRequested { get; }
            internal bool DirectSqeDisabled { get; }
            internal bool ZeroCopySendOptedIn { get; }
            internal bool RegisterBuffersEnabled { get; }
            internal bool AdaptiveProvidedBufferSizingEnabled { get; }
            internal int ProvidedBufferSize { get; }
            internal int PrepareQueueCapacity { get; }
            internal int CancellationQueueCapacity { get; }
            private readonly IoUringConfigurationWarningFlags _warningFlags;

            internal IoUringResolvedConfiguration(
                bool ioUringEnabled,
                bool sqPollRequested,
                bool directSqeDisabled,
                bool zeroCopySendOptedIn,
                bool registerBuffersEnabled,
                bool adaptiveProvidedBufferSizingEnabled,
                int providedBufferSize,
                int prepareQueueCapacity,
                int cancellationQueueCapacity)
            {
                IoUringEnabled = ioUringEnabled;
                SqPollRequested = sqPollRequested;
                DirectSqeDisabled = directSqeDisabled;
                ZeroCopySendOptedIn = zeroCopySendOptedIn;
                RegisterBuffersEnabled = registerBuffersEnabled;
                AdaptiveProvidedBufferSizingEnabled = adaptiveProvidedBufferSizingEnabled;
                ProvidedBufferSize = providedBufferSize;
                PrepareQueueCapacity = prepareQueueCapacity;
                CancellationQueueCapacity = cancellationQueueCapacity;
                _warningFlags = ComputeWarningFlags(
                    ioUringEnabled,
                    sqPollRequested,
                    directSqeDisabled,
                    zeroCopySendOptedIn);
            }

            internal string ToLogString() =>
                $"enabled={IoUringEnabled}, sqpollRequested={SqPollRequested}, directSqeDisabled={DirectSqeDisabled}, zeroCopySendOptedIn={ZeroCopySendOptedIn}, registerBuffersEnabled={RegisterBuffersEnabled}, adaptiveProvidedBufferSizingEnabled={AdaptiveProvidedBufferSizingEnabled}, providedBufferSize={ProvidedBufferSize}, prepareQueueCapacity={PrepareQueueCapacity}, cancellationQueueCapacity={CancellationQueueCapacity}";

            internal bool TryGetValidationWarnings([NotNullWhen(true)] out string? warnings)
            {
                if (_warningFlags == IoUringConfigurationWarningFlags.None)
                {
                    warnings = null;
                    return false;
                }

                warnings = BuildWarningMessage(_warningFlags);
                return true;
            }

            private static IoUringConfigurationWarningFlags ComputeWarningFlags(
                bool ioUringEnabled, bool sqPollRequested, bool directSqeDisabled, bool zeroCopySendOptedIn)
            {
                if (ioUringEnabled)
                {
                    return IoUringConfigurationWarningFlags.None;
                }

                IoUringConfigurationWarningFlags warnings = IoUringConfigurationWarningFlags.None;
                if (sqPollRequested)    warnings |= IoUringConfigurationWarningFlags.SqPollRequestedWithoutIoUring;
                if (directSqeDisabled)  warnings |= IoUringConfigurationWarningFlags.DirectSqeDisabledWithoutIoUring;
                if (zeroCopySendOptedIn) warnings |= IoUringConfigurationWarningFlags.ZeroCopyOptInWithoutIoUring;
                return warnings;
            }

            private static string BuildWarningMessage(IoUringConfigurationWarningFlags warnings)
            {
                var parts = new List<string>(3);
                if ((warnings & IoUringConfigurationWarningFlags.SqPollRequestedWithoutIoUring) != 0)
                {
                    parts.Add("SQPOLL requested while io_uring is disabled");
                }

                if ((warnings & IoUringConfigurationWarningFlags.DirectSqeDisabledWithoutIoUring) != 0)
                {
                    parts.Add("direct SQE disabled while io_uring is disabled");
                }

                if ((warnings & IoUringConfigurationWarningFlags.ZeroCopyOptInWithoutIoUring) != 0)
                {
                    parts.Add("zero-copy send opted-in while io_uring is disabled");
                }

                return string.Join("; ", parts);
            }
        }

        /// <summary>Mirrors kernel <c>struct io_uring_sqe</c> (64 bytes), written to the SQ ring for submission.</summary>
        [StructLayout(LayoutKind.Explicit, Size = 64)]
        internal struct IoUringSqe
        {
            [FieldOffset(0)]
            internal byte Opcode;
            [FieldOffset(1)]
            internal byte Flags;
            [FieldOffset(2)]
            internal ushort Ioprio;
            [FieldOffset(4)]
            internal int Fd;
            [FieldOffset(8)]
            internal ulong Off;
            [FieldOffset(16)]
            internal ulong Addr;
            [FieldOffset(24)]
            internal uint Len;
            [FieldOffset(28)]
            internal uint RwFlags;
            [FieldOffset(32)]
            internal ulong UserData;
            [FieldOffset(40)]
            internal ushort BufIndex;
            [FieldOffset(42)]
            internal ushort Personality;
            [FieldOffset(44)]
            internal int SpliceFdIn;
            [FieldOffset(48)]
            internal ulong Addr3;
        }

        /// <summary>Mirrors kernel <c>struct io_uring_probe_op</c> (8 bytes per entry in the probe ops array).</summary>
        [StructLayout(LayoutKind.Explicit, Size = 8)]
        private struct IoUringProbeOp
        {
            [FieldOffset(0)] internal byte Op;
            [FieldOffset(1)] internal byte Resv;
            [FieldOffset(2)] internal ushort Flags;
            // 4 bytes reserved at offset 4
        }

        /// <summary>Mirrors kernel <c>struct io_uring_probe</c> (16-byte header preceding the variable-length ops array).</summary>
        [StructLayout(LayoutKind.Explicit, Size = 16)]
        private struct IoUringProbeHeader
        {
            [FieldOffset(0)] internal byte LastOp;
            [FieldOffset(1)] internal byte OpsLen;
            // 14 bytes reserved at offset 2
        }

        /// <summary>Captures the results of <c>io_uring_setup(2)</c> including ring fd, negotiated params, and feature flags.</summary>
        private struct IoUringSetupResult
        {
            internal int RingFd;
            internal Interop.Sys.IoUringParams Params;
            internal uint NegotiatedFlags;
            internal bool UsesExtArg;
        }

        /// <summary>Discriminates completion slot metadata shape for operation-specific post-completion processing.</summary>
        private enum IoUringCompletionOperationKind : byte
        {
            None = 0,
            Accept = 1,
            Message = 2,
            ReusePortAccept = 3,
            PollReadiness = 4,
        }

        /// <summary>
        /// Hot per-slot metadata used on every CQE dispatch.
        /// Keep this minimal; native pointer-heavy state is kept in <see cref="IoUringCompletionSlotStorage"/>.
        /// Explicit 24-byte layout keeps generation/free-list state and hot flags in one compact block.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = 24)]
        private struct IoUringCompletionSlot
        {
            // 0..7
            [FieldOffset(0)]
            public ulong Generation;
            // 8..11 (-1 = end of free list)
            [FieldOffset(8)]
            public int FreeListNext;
            // 12..15 (operation kind + hot state flags)
            [FieldOffset(12)]
            private uint _packedState;
            // 16..17
            [FieldOffset(16)]
            public ushort FixedRecvBufferId;
#if DEBUG
            // 20..23 debug-only forced completion result payload.
            [FieldOffset(20)]
            public int TestForcedResult;
#endif

            private const uint KindMask = 0xFFu;
            private const uint FlagIsZeroCopySend = 1u << 8;
            private const uint FlagZeroCopyNotificationPending = 1u << 9;
            private const uint FlagUsesFixedRecvBuffer = 1u << 10;
#if DEBUG
            private const uint FlagHasTestForcedResult = 1u << 11;
#endif

            public IoUringCompletionOperationKind Kind
            {
                get => (IoUringCompletionOperationKind)(_packedState & KindMask);
                set => _packedState = (_packedState & ~KindMask) | ((uint)value & KindMask);
            }

            public bool IsZeroCopySend
            {
                get => (_packedState & FlagIsZeroCopySend) != 0;
                set => SetFlag(FlagIsZeroCopySend, value);
            }

            public bool ZeroCopyNotificationPending
            {
                get => (_packedState & FlagZeroCopyNotificationPending) != 0;
                set => SetFlag(FlagZeroCopyNotificationPending, value);
            }

            public bool UsesFixedRecvBuffer
            {
                get => (_packedState & FlagUsesFixedRecvBuffer) != 0;
                set => SetFlag(FlagUsesFixedRecvBuffer, value);
            }

#if DEBUG
            public bool HasTestForcedResult
            {
                get => (_packedState & FlagHasTestForcedResult) != 0;
                set => SetFlag(FlagHasTestForcedResult, value);
            }
#endif

            private void SetFlag(uint mask, bool value)
            {
                if (value)
                {
                    _packedState |= mask;
                }
                else
                {
                    _packedState &= ~mask;
                }
            }

            /// <summary>Clears both zero-copy flags (single bitmask operation).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void ClearZeroCopyState() =>
                _packedState &= ~(FlagIsZeroCopySend | FlagZeroCopyNotificationPending);

            /// <summary>Arms the slot for a SEND_ZC operation: sets IsZeroCopySend, clears NotificationPending.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void ArmZeroCopySend() =>
                _packedState = (_packedState | FlagIsZeroCopySend) & ~FlagZeroCopyNotificationPending;
        }

        /// <summary>
        /// Hot tracked-operation ownership state used on completion and cancellation paths.
        /// Kept separate from native slot storage to improve cache locality in CQE dispatch.
        /// </summary>
        private struct IoUringTrackedOperationState
        {
            public SocketAsyncContext.AsyncOperation? TrackedOperation;
            public ulong TrackedOperationGeneration;
        }

        /// <summary>
        /// Cold per-slot native metadata: pointers and message writeback state needed only for
        /// operation-specific completion processing.
        /// </summary>
        private struct IoUringCompletionSlotStorage
        {
            // Hold a DangerousAddRef lease for the socket fd until this slot is fully retired.
            public SafeSocketHandle? DangerousRefSocketHandle;
            // Per-slot pre-allocated native slab backing accept socklen_t and message inline storage.
            public unsafe byte* NativeInlineStorage;
            // Accept metadata
            public unsafe int* NativeSocketAddressLengthPtr; // socklen_t* in NativeInlineStorage
            // Message metadata (pointers to native-alloc'd msghdr/iovec)
            public IntPtr NativeMsgHdrPtr;
            public bool MessageIsReceive;
            // Message metadata - deep-copied native msghdr constituents (point into NativeInlineStorage).
            public unsafe Interop.Sys.IOVector* NativeIOVectors;
            public unsafe byte* NativeSocketAddress;
            public unsafe byte* NativeControlBuffer;
            // RecvMsg output capture - pointers back to managed MessageHeader buffers for writeback
            public unsafe byte* ReceiveOutputSocketAddress;
            public unsafe byte* ReceiveOutputControlBuffer;
            public int ReceiveSocketAddressCapacity;
            public int ReceiveControlBufferCapacity;
            // ReusePortAccept metadata - cross-engine references for shadow listener accept forwarding
            public SocketAsyncContext? ReusePortPrimaryContext;
            public SocketAsyncEngine? ReusePortPrimaryEngine;
        }

        /// <summary>
        /// Mirrors the kernel's <c>struct msghdr</c> layout for direct SQE submission.
        /// Used by <see cref="TryPrepareInlineMessageStorage"/> to build a native msghdr that
        /// io_uring sendmsg/recvmsg opcodes can consume directly.
        /// Must only be used on 64-bit Linux where sizeof(msghdr) == 56.
        /// </summary>
        [StructLayout(LayoutKind.Explicit)]
        private unsafe struct NativeMsghdr
        {
            /// <summary>Expected size of the kernel's <c>struct msghdr</c> on 64-bit Linux.</summary>
            public const int ExpectedSize = 56;

            [FieldOffset(0)]
            public void* MsgName;
            [FieldOffset(8)]
            public uint MsgNameLen;
            [FieldOffset(16)]
            public Interop.Sys.IOVector* MsgIov;
            [FieldOffset(24)]
            public nuint MsgIovLen;
            [FieldOffset(32)]
            public void* MsgControl;
            [FieldOffset(40)]
            public nuint MsgControlLen;
            [FieldOffset(48)]
            public int MsgFlags;
        }

        /// <summary>
        /// Managed ring mmap state. Accessed directly as <c>_ringState.*</c> throughout the engine.
        /// </summary>
        private unsafe struct ManagedRingState
        {
            public Interop.Sys.IoUringCqe* CqeBase;
            public uint* CqTailPtr;
            public uint* CqHeadPtr;
            public uint CqMask;
            public uint CqEntries;
            public uint* CqOverflowPtr;
            public uint ObservedCqOverflow;
            public byte* SqRingPtr;
            public byte* CqRingPtr;
            public uint* SqFlagsPtr;
            public ulong SqRingSize;
            public ulong CqRingSize;
            public ulong SqesSize;
            public bool UsesSingleMmap;
            public int RingFd;
            public bool UsesExtArg;
            public bool UsesNoSqArray;
            public uint NegotiatedFlags;
            public uint CachedCqHead;
            public bool CqDrainEnabled;
            public int WakeupEventFd;

            public static ManagedRingState CreateDefault()
            {
                ManagedRingState state = default;
                state.RingFd = -1;
                state.WakeupEventFd = -1;
                return state;
            }
        }

        [StructLayout(LayoutKind.Sequential, Size = 64)]
        private struct CacheLinePadding64
        {
        }

        private readonly struct ReusePortShadowSetupRequest
        {
            public readonly SafeSocketHandle ShadowSocket;
            public readonly SocketAsyncContext PrimaryContext;
            public readonly SocketAsyncEngine PrimaryEngine;

            public ReusePortShadowSetupRequest(SafeSocketHandle shadowSocket, SocketAsyncContext primaryContext, SocketAsyncEngine primaryEngine)
            {
                ShadowSocket = shadowSocket;
                PrimaryContext = primaryContext;
                PrimaryEngine = primaryEngine;
            }
        }

        /// <summary>Queued work item pairing an operation with its prepare sequence number for deferred SQE preparation.</summary>
        private readonly struct IoUringPrepareWorkItem
        {
            /// <summary>The operation to prepare.</summary>
            public readonly SocketAsyncContext.AsyncOperation Operation;
            /// <summary>The sequence number that must match for the preparation to proceed.</summary>
            public readonly long PrepareSequence;

            /// <summary>Creates a work item pairing an operation with its prepare sequence number.</summary>
            public IoUringPrepareWorkItem(SocketAsyncContext.AsyncOperation operation, long prepareSequence)
            {
                Operation = operation;
                PrepareSequence = prepareSequence;
            }
        }
    }
}
