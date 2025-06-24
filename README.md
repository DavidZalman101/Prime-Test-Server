# Prime Server

A single-threaded server that checks if numbers are prime using libaco coroutines and io_uring.

## Requirements

**Protocol**: Unix socket `/tmp/prime_server.sock`, input "number (as string)\n", output "Y\n" or "N\n"

**Two modes**
- `./prime_server --throughput` - maximize requests/second
- `./prime_server --low-latency` - minimize p99 latency

**Constraints**:
- Single-threaded, max 128 concurrent clients
- Use libaco for coroutines, io_uring for ALL I/O
- Must check all odd divisors 3 to n (no algorithm optimizations and no caching)

**Key**: Small primes must stay fast even when checking large primes

## Build & Test

```bash
make                                 # Auto-downloads libaco
./prime_server --throughput          # Start server (you can run in a separate
                                     # terminal or as a background process)
echo "17" | nc -U /tmp/prime_server.sock

# Benchmark
./benchmark_client -n 10000 -t 1     # Throughput test
./benchmark_client -n 1000 -t 4 -r   # Latency test
```

## Resources
- **io_uring**
- **libaco**
