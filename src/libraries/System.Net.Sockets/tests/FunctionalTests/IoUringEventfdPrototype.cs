using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

class Runner
{
    static async Task Main()
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(1);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;
        var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        client.Connect(new IPEndPoint(IPAddress.Loopback, port));
        var server = listener.Accept();
        int cfd = client.SafeHandle.DangerousGetHandle().ToInt32();
        int sfd = server.SafeHandle.DangerousGetHandle().ToInt32();
        Console.WriteLine($"Sockets: cfd={cfd} sfd={sfd}");

        Ring.Setup();

        var pollerCts = new CancellationTokenSource();
        _ = Task.Run(() => Ring.PollForCompletions(pollerCts.Token));

        byte[] msg = Encoding.UTF8.GetBytes("hello via eventfd!");

        Console.Write("SEND... ");
        int n = await Ring.SubmitAsync(26, cfd, msg, 1);
        Console.WriteLine($"sent {n} bytes");

        Console.Write("RECV... ");
        byte[] rb = new byte[64];
        n = await Ring.SubmitAsync(27, sfd, rb, 2);
        Console.WriteLine($"recv {n} bytes: '{Encoding.UTF8.GetString(rb, 0, n)}'");

        Console.Write("Ping-pong 1000... ");
        byte[] tb = new byte[64];
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 1000; i++)
        {
            await Ring.SubmitAsync(26, cfd, msg, (ulong)(i * 2 + 100));
            await Ring.SubmitAsync(27, sfd, tb, (ulong)(i * 2 + 101));
        }
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMicroseconds / 1000:F1} us/rt");

        Console.Write("Concurrent 200 send+recv... ");
        int count = 200;
        long total = count * msg.Length;
        var recvTask = Task.Run(async () =>
        {
            long r = 0;
            byte[] b = new byte[4096];
            while (r < total)
            {
                int x = await Ring.SubmitAsync(27, sfd, b, (ulong)(20000 + r));
                if (x <= 0) break;
                r += x;
            }
            return r;
        });
        for (int i = 0; i < count; i++)
            await Ring.SubmitAsync(26, cfd, msg, (ulong)(10000 + i));
        long got = await recvTask;
        Console.WriteLine($"sent={total} recv={got}");

        pollerCts.Cancel();
        Console.WriteLine("Done!");
    }
}

unsafe static class Ring
{
    [DllImport("libc", EntryPoint = "syscall", SetLastError = true)]
    static extern int sys_setup(long nr, uint entries, Params* p);
    [DllImport("libc", EntryPoint = "syscall", SetLastError = true)]
    static extern int sys_enter(long nr, int fd, uint submit, uint min, uint flags, void* a, nint s);
    [DllImport("libc", EntryPoint = "syscall", SetLastError = true)]
    static extern int sys_register(long nr, int fd, uint op, void* arg, uint n);
    [DllImport("libc")] static extern void* mmap(void* a, ulong len, int prot, int flags, int fd, long off);
    [DllImport("libc")] static extern int eventfd(uint init, int flags);
    [DllImport("libc")] static extern nint read(int fd, void* buf, nuint count);
    [DllImport("libc")] static extern int poll(PollFd* fds, uint nfds, int timeout);

    [StructLayout(LayoutKind.Sequential)]
    struct PollFd { public int fd; public short events; public short revents; }

    [StructLayout(LayoutKind.Sequential)]
    struct Params
    {
        public uint sq_entries, cq_entries, flags, sq_thread_cpu, sq_thread_idle, features, wq_fd, r0, r1, r2;
        public uint sq_h, sq_t, sq_rm, sq_re, sq_f, sq_d, sq_a, sq_r1, sq_r2, sq_ua;
        public uint cq_h, cq_t, cq_rm, cq_re, cq_o, cq_c, cq_f, cq_r1, cq_r2;
    }

    [StructLayout(LayoutKind.Explicit, Size = 64)]
    struct Sqe
    {
        [FieldOffset(0)] public byte opcode;
        [FieldOffset(4)] public int fd;
        [FieldOffset(16)] public ulong addr;
        [FieldOffset(24)] public uint len;
        [FieldOffset(32)] public ulong user_data;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct Cqe { public ulong user_data; public int res; public uint flags; }

    static Sqe* sqes;
    static uint* sqTail, sqArr;
    static uint sqMask;
    static uint* cqHead, cqTailP;
    static Cqe* cqeBase;
    static uint cqMask;
    static int ringFd, evFd;
    static TaskCompletionSource<int>?[] pending = new TaskCompletionSource<int>[65536];
    static readonly Lock sqLock = new Lock(); // serializes SQ tail access for concurrent submitters

    internal static void Setup()
    {
        Params p = default;
        p.flags = 0x100; // COOP_TASKRUN
        ringFd = sys_setup(425, 256, &p);
        Console.WriteLine($"Ring fd={ringFd}");

        byte* sq = (byte*)mmap(null, p.sq_a + p.sq_entries * 4, 3, 0x8001, ringFd, 0);
        byte* cq = (byte*)mmap(null, p.cq_c + p.cq_entries * (uint)sizeof(Cqe), 3, 0x8001, ringFd, 0x8000000);
        sqes = (Sqe*)mmap(null, p.sq_entries * (uint)sizeof(Sqe), 3, 0x8001, ringFd, 0x10000000);
        sqTail = (uint*)(sq + p.sq_t);
        sqArr = (uint*)(sq + p.sq_a);
        sqMask = p.sq_entries - 1;
        cqHead = (uint*)(cq + p.cq_h);
        cqTailP = (uint*)(cq + p.cq_t);
        cqeBase = (Cqe*)(cq + p.cq_c);
        cqMask = p.cq_entries - 1;

        evFd = eventfd(0, 0x800); // EFD_NONBLOCK
        int fd = evFd;
        int r = sys_register(427, ringFd, 4, &fd, 1); // IORING_REGISTER_EVENTFD
        Console.WriteLine($"eventfd={evFd} register={r}");
    }

    internal static Task<int> SubmitAsync(byte opcode, int fd, byte[] buffer, ulong id)
    {
        var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        pending[id & 0xFFFF] = tcs;

        fixed (byte* buf = buffer)
        {
            lock (sqLock)
            {
                uint t = Volatile.Read(ref *sqTail);
                Sqe* s = &sqes[t & sqMask];
                *s = default;
                s->opcode = opcode;
                s->fd = fd;
                s->addr = (ulong)buf;
                s->len = (uint)buffer.Length;
                s->user_data = id;
                sqArr[t & sqMask] = t & sqMask;
                Volatile.Write(ref *sqTail, t + 1);
            }
        }

        sys_enter(426, ringFd, 1, 0, 0, null, 0);
        return tcs.Task;
    }

    internal static void PollForCompletions(CancellationToken ct)
    {
        PollFd pfd;
        pfd.fd = evFd;
        pfd.events = 1; // POLLIN

        while (!ct.IsCancellationRequested)
        {
            pfd.revents = 0;
            if (poll(&pfd, 1, 50) <= 0) continue;

            ulong val;
            read(evFd, &val, 8);

            while (true)
            {
                uint h = Volatile.Read(ref *cqHead);
                uint t = Volatile.Read(ref *cqTailP);
                if (h == t) break;

                Cqe* cqe = &cqeBase[h & cqMask];
                ulong ud = cqe->user_data;
                int res = cqe->res;
                Volatile.Write(ref *cqHead, h + 1);

                var tcs = Interlocked.Exchange(ref pending[ud & 0xFFFF], null);
                if (tcs != null)
                {
                    if (res >= 0) tcs.TrySetResult(res);
                    else tcs.TrySetException(new Exception($"io_uring error: {res}"));
                }
            }
        }
    }
}
