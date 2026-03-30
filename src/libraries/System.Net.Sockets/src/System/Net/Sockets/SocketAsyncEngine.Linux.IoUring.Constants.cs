// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

namespace System.Net.Sockets
{
    internal sealed unsafe partial class SocketAsyncEngine
    {
        /// <summary>
        /// Kernel ABI opcode constants as a static class (not an enum) to avoid byte-cast noise
        /// at every SQE write site, since the SQE Opcode field is typed as byte.
        /// </summary>
        private static class IoUringOpcodes
        {
            internal const byte ReadFixed = 4;
            internal const byte Send = 26;
            internal const byte Recv = 27;
            internal const byte SendMsg = 9;
            internal const byte RecvMsg = 10;
            internal const byte Accept = 13;
            internal const byte Connect = 16;
            internal const byte SendZc = 53;
            internal const byte SendMsgZc = 54;
            internal const byte AsyncCancel = 14;
            internal const byte PollAdd = 6;
        }

        /// <summary>
        /// Centralizes io_uring ABI constants that mirror the native definitions in pal_io_uring.c.
        /// These are used by managed code that directly interacts with the io_uring submission
        /// and completion rings (e.g., direct SQE writes via mmap'd ring access).
        /// </summary>
        private static class IoUringConstants
        {
            // Setup flags (io_uring_setup params.flags)
            internal const uint SetupCqSize       = 1u << 3;
            internal const uint SetupSqPoll       = 1u << 5;
            internal const uint SetupSubmitAll    = 1u << 7;
            internal const uint SetupCoopTaskrun  = 1u << 8;
            internal const uint SetupSqe128       = 1u << 10;
            internal const uint SetupSingleIssuer = 1u << 12;
            internal const uint SetupDeferTaskrun = 1u << 13;
            internal const uint SetupRDisabled    = 1u << 6;
            internal const uint SetupNoSqArray    = 1u << 16;
            internal const uint SetupCloexec      = 1u << 19;

            // Feature flags (io_uring_params.features)
            internal const uint FeatureSingleMmap = 1u << 0;
            internal const uint FeatureExtArg = 1u << 8;

            // Enter flags (io_uring_enter flags parameter)
            internal const uint EnterGetevents      = 1u << 0;
            internal const uint EnterSqWakeup       = 1u << 1;
            internal const uint EnterExtArg         = 1u << 3;
            internal const uint EnterRegisteredRing = 1u << 4;

            // SQ ring flags (sq_ring->flags)
            internal const uint SqNeedWakeup = 1u << 0;

            // Register opcodes
            internal const uint RegisterEnableRings      = 17;
            internal const uint RegisterBuffers          = 0;
            internal const uint UnregisterBuffers        = 1;
            internal const uint RegisterProbe            = 8;
            internal const uint RegisterRingFds          = 20;
            internal const uint UnregisterRingFds        = 21;
            internal const uint RegisterPbufRing         = 22;
            internal const uint UnregisterPbufRing       = 23;

            // Register helper values
            internal const uint RegisterOffsetAuto = 0xFFFFFFFFU;

            // Probe op flags
            internal const uint ProbeOpFlagSupported = 1u << 0;

            // Poll flags
            internal const uint PollAddFlagMulti = 1u << 0;
            internal const uint PollIn = 0x0001;
            internal const uint PollOut = 0x0004;

            // CQE flags
            internal const uint CqeFBuffer = 1u << 0; // IORING_CQE_F_BUFFER (buffer id in upper bits)
            internal const uint CqeFMore = 1u << 1; // IORING_CQE_F_MORE (multishot)
            internal const uint CqeFSockNonEmpty = 1u << 2; // IORING_CQE_F_SOCK_NONEMPTY (more data pending after recv)
            internal const uint CqeFNotif = 1u << 3; // IORING_CQE_F_NOTIF (zero-copy notification)
            internal const int CqeBufferShift = 16; // IORING_CQE_BUFFER_SHIFT

            // Send/Recv ioprio flags
            // IORING_RECVSEND_POLL_FIRST: skip the initial non-blocking attempt and go
            // straight to kernel-internal FAST_POLL. This avoids EAGAIN for O_NONBLOCK sockets
            // and eliminates the need for userspace EAGAIN retry logic. (Linux 5.19+)
            internal const ushort RecvSendPollFirst = 1 << 0; // IORING_RECVSEND_POLL_FIRST
            internal const ushort RecvMultishot = 1 << 1; // IORING_RECV_MULTISHOT
            // Accept ioprio flags
            internal const ushort AcceptMultishot = 1 << 0; // IORING_ACCEPT_MULTISHOT

            // SQE flags
            internal const byte SqeBufferSelect = 1 << 5; // IOSQE_BUFFER_SELECT

            // Sizing
            internal const uint QueueEntries = 1024;
            // Keep CQ capacity at 4x SQ entries to absorb completion bursts during short GC pauses
            // without immediately tripping overflow recovery on busy rings.
            internal const uint CqEntriesFactor = 4;
            internal const uint MaxCqeDrainBatch = 512;
            internal const int CqePrefetchThreshold = 4;
            // Bounded wait trades wake latency for starvation resilience:
            // if an eventfd wake is missed or deferred, the event loop still polls at least once
            // every 50ms (worst-case deferred wake latency).
            internal const long BoundedWaitTimeoutNanos = 50L * 1000 * 1000; // 50ms
            // Circuit-breaker bounded wait used after repeated eventfd wake failures.
            internal const long WakeFailureFallbackWaitTimeoutNanos = 1L * 1000 * 1000; // 1ms

            // Completion operation pool sizing
            internal const int CompletionOperationPoolCapacityFactor = 2;

            // mmap offsets (from kernel UAPI: IORING_OFF_SQ_RING, IORING_OFF_CQ_RING, IORING_OFF_SQES)
            internal const ulong OffSqRing = 0;
            internal const ulong OffCqRing = 0x8000000;
            internal const ulong OffSqes   = 0x10000000;

            // Minimum kernel version for io_uring engine.
            // SEND_ZC deferred-completion logic relies on NOTIF CQE sequencing behavior stabilized in Linux 6.1.0.
            internal const int MinKernelMajor = 6;
            internal const int MinKernelMinor = 1;

            // Zero-copy send size threshold (payloads below this use regular send).
            // Disabled: SEND_ZC NOTIF CQE handling has a bug that causes EINVAL after ~40
            // sequential large sends. Regular SEND works correctly for all sizes.
            internal const int ZeroCopySendThreshold = int.MaxValue;

            // User data tag values (encoded in upper bits of user_data)
            internal const byte TagNone               = 0;
            internal const byte TagReservedCompletion = 2;
            internal const byte TagWakeupSignal       = 3;

            // Accept-time flags for accepted socket descriptors: SOCK_CLOEXEC | SOCK_NONBLOCK.
            internal const uint AcceptFlags = 0x80800;

            // Message inline capacities (avoid heap allocation on common small payloads)
            internal const int MessageInlineIovCount = 4;
            internal const int MessageInlineSocketAddressCapacity = 128; // sizeof(sockaddr_storage)
            internal const int MessageInlineControlBufferCapacity = 128;

            // Internal discriminator for io_uring vs epoll fallback detection
            internal const int NotSocketEventPort = int.MinValue + 1;

            // Completion slot encoding
            // Slot index is encoded into 16 bits of user_data payload => max 65536 slot IDs per engine.
            internal const int SlotIndexBits = 16;
            internal const ulong SlotIndexMask = (1UL << SlotIndexBits) - 1UL;
            internal const int GenerationBits = 56 - SlotIndexBits;
            // 40-bit generation space gives each slot ~1.1 trillion incarnations before wrap.
            // Generation zero remains reserved as "uninitialized", so wrap remaps 2^40-1 -> 1.
            internal const ulong GenerationMask = (1UL << GenerationBits) - 1UL;

            // Test hook opcode masks (mirrors IoUringTestOpcodeMask in pal_io_uring.c)
            internal const byte TestOpcodeMaskNone = 0;
            internal const byte TestOpcodeMaskSend = 1 << 0;
            internal const byte TestOpcodeMaskRecv = 1 << 1;
            internal const byte TestOpcodeMaskSendMsg = 1 << 2;
            internal const byte TestOpcodeMaskRecvMsg = 1 << 3;
            internal const byte TestOpcodeMaskAccept = 1 << 4;
            internal const byte TestOpcodeMaskConnect = 1 << 5;
            internal const byte TestOpcodeMaskSendZc = 1 << 6;
            internal const byte TestOpcodeMaskSendMsgZc = 1 << 7;
        }
    }
}
