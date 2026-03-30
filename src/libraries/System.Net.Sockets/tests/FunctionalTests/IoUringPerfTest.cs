using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

class Benchmark
{
    static async Task Main()
    {
        string mode = Environment.GetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_IO_URING") == "1"
            ? "io_uring" : "epoll";
        Console.WriteLine($"Backend: {mode} | Cores: {Environment.ProcessorCount} | Kernel: {Environment.OSVersion}");
        Console.WriteLine();

        await PingPongLatency();
        await BulkThroughput();
        await ConnectAcceptRate();
        await ConcurrentPingPong();
    }

    static async Task PingPongLatency()
    {
        Console.WriteLine("=== Ping-Pong Latency (1 byte, single connection) ===");
        var (c, s, l) = await MakePair();
        byte[] sb = new byte[1], rb = new byte[1];

        // Warmup
        for (int i = 0; i < 2000; i++)
        { await c.SendAsync(sb); await s.ReceiveAsync(rb); await s.SendAsync(rb); await c.ReceiveAsync(rb); }

        foreach (int n in new[] { 10_000, 50_000, 100_000 })
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            { await c.SendAsync(sb); await s.ReceiveAsync(rb); await s.SendAsync(rb); await c.ReceiveAsync(rb); }
            sw.Stop();
            double usPerRt = sw.Elapsed.TotalMicroseconds / n;
            double opsPerSec = n / sw.Elapsed.TotalSeconds;
            Console.WriteLine($"  {n,7}: {usPerRt,7:F1} us/rt  {opsPerSec,10:F0} ops/s");
        }
        c.Dispose(); s.Dispose(); l.Dispose();
        Console.WriteLine();
    }

    static async Task BulkThroughput()
    {
        Console.WriteLine("=== Bulk Throughput (concurrent send + recv) ===");
        foreach (int chunkSize in new[] { 1024, 4096, 16384 })
        {
            var (c, s, l) = await MakePair();
            byte[] sb = new byte[chunkSize], rb = new byte[65536];
            int count = Math.Max(500, 400_000 / chunkSize);
            long totalBytes = (long)count * chunkSize;

            // Warmup
            for (int i = 0; i < 100; i++)
            { await c.SendAsync(sb); int t = 0; while (t < chunkSize) t += await s.ReceiveAsync(rb.AsMemory(t)); }

            var recv = Task.Run(async () =>
            { long r = 0; while (r < totalBytes) { int n = await s.ReceiveAsync(rb); if (n == 0) break; r += n; } return r; });

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) await c.SendAsync(sb);

            long received = 0;
            if (await Task.WhenAny(recv, Task.Delay(30000)) == recv)
            { sw.Stop(); received = await recv; }
            else sw.Stop();

            if (received == totalBytes)
            {
                double mbps = (totalBytes / (1024.0 * 1024.0)) / sw.Elapsed.TotalSeconds;
                Console.WriteLine($"  {chunkSize / 1024,3}KB x {count,5}: {mbps,7:F0} MB/s  ({sw.Elapsed.TotalMilliseconds:F0}ms)");
            }
            else
                Console.WriteLine($"  {chunkSize / 1024,3}KB x {count,5}: TIMEOUT ({received}/{totalBytes})");

            c.Dispose(); s.Dispose(); l.Dispose();
        }
        Console.WriteLine();
    }

    static async Task ConnectAcceptRate()
    {
        Console.WriteLine("=== Connect/Accept Rate ===");
        var l = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        l.Bind(new IPEndPoint(IPAddress.Loopback, 0)); l.Listen(512);
        var ep = (IPEndPoint)l.LocalEndPoint!;

        // Warmup
        for (int i = 0; i < 500; i++)
        { var x = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp); await x.ConnectAsync(ep); (await l.AcceptAsync()).Dispose(); x.Dispose(); }

        foreach (int n in new[] { 5000, 10000 })
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            { var x = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp); await x.ConnectAsync(ep); (await l.AcceptAsync()).Dispose(); x.Dispose(); }
            sw.Stop();
            Console.WriteLine($"  {n,5}: {n / sw.Elapsed.TotalSeconds,8:F0} conn/s  {sw.Elapsed.TotalMicroseconds / n,7:F1} us/conn");
        }
        l.Dispose();
        Console.WriteLine();
    }

    static async Task ConcurrentPingPong()
    {
        Console.WriteLine("=== Concurrent Ping-Pong (1 byte, N connections) ===");
        var ll = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        ll.Bind(new IPEndPoint(IPAddress.Loopback, 0)); ll.Listen(512);

        foreach (int conc in new[] { 1, 10, 50, 100, 200 })
        {
            int ipc = 5000;
            var pairs = new (Socket c, Socket s)[conc];
            for (int i = 0; i < conc; i++)
            {
                var x = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                x.NoDelay = true;
                await x.ConnectAsync((IPEndPoint)ll.LocalEndPoint!);
                var y = await ll.AcceptAsync(); y.NoDelay = true;
                pairs[i] = (x, y);
            }

            // Warmup
            await Task.WhenAll(pairs.Select(p => PingPong(p.c, p.s, 200)));

            var sw = Stopwatch.StartNew();
            await Task.WhenAll(pairs.Select(p => PingPong(p.c, p.s, ipc)));
            sw.Stop();

            long tot = (long)conc * ipc;
            Console.WriteLine($"  {conc,3} conns x {ipc}: {tot / sw.Elapsed.TotalSeconds,10:F0} ops/s  {sw.Elapsed.TotalMicroseconds / tot,7:F1} us/op");
            foreach (var (x, y) in pairs) { x.Dispose(); y.Dispose(); }
        }
        ll.Dispose();
        Console.WriteLine();
    }

    static async Task PingPong(Socket c, Socket s, int n)
    {
        byte[] a = new byte[1], b = new byte[1];
        for (int i = 0; i < n; i++)
        { await c.SendAsync(a); await s.ReceiveAsync(b); await s.SendAsync(b); await c.ReceiveAsync(b); }
    }

    static async Task<(Socket, Socket, Socket)> MakePair()
    {
        var l = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        l.Bind(new IPEndPoint(IPAddress.Loopback, 0)); l.Listen(1);
        var c = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        c.NoDelay = true;
        await c.ConnectAsync((IPEndPoint)l.LocalEndPoint!);
        var s = await l.AcceptAsync(); s.NoDelay = true;
        return (c, s, l);
    }
}
