# PulseFeed

A Linux C++20 relay for synthetic market-data updates. One publisher sends a TCP stream, and the relay broadcasts updates to each connected subscriber. The benchmark compares a bounded SPSC ring with a mutex-protected queue and shows how subscriber count and slow consumers affect delivery and latency.

## Build and run

Requires Linux, a C++20 compiler, CMake 3.16+, and Python 3 for the integration test and benchmark runner.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Start these commands in three terminals, in order. Wait for each `READY` line before continuing.

```sh
./build/pulsefeed --ingress 9001 --egress 9002 --queue spsc
./build/subscriber 9002
./build/publisher 9001 5000 3
```

Stop the relay with Ctrl+C after the publisher finishes. The relay and subscriber print JSON summaries. Add more subscribers before starting the publisher to try fanout. Run `./build/pulsefeed --help` for queue, buffer, and shutdown options.

## Protocol and threading

Each update is a 32-byte binary frame. All fields are unsigned and big-endian.

| Byte offset | Width | Field |
|---:|---:|---|
| 0 | 8 | Sequence number, starting at 1 per publisher session |
| 8 | 4 | Instrument ID |
| 12 | 8 | Price in integer ticks |
| 20 | 4 | Quantity |
| 24 | 8 | Publisher monotonic timestamp in nanoseconds |

Both listeners bind to IPv4 loopback. The ingress thread accepts one active publisher, reconstructs frames in an 8 KiB staging buffer, and enqueues them. The fanout thread owns subscriber sockets, broadcasts frames in order, and tracks each socket’s pending bytes and partial sends. The two threads use level-triggered epoll and eventfd notifications. Subscribers receive updates after acceptance, and sequences are scoped to one publisher connection.

When the ingest queue fills, the relay pauses publisher reads until the queue drops to half capacity. Each subscriber has a capped pending buffer and a per-event write budget. If a buffer cannot fit the next frame, the relay closes that subscriber and increments its disconnect counter. There is no replay. On SIGINT or SIGTERM, it closes admission and drains staged, queued, and pending data until the deadline. The shutdown counters cover complete staged or queued frames and pending subscriber deliveries discarded in userspace. They exclude bytes the process has yet to read and bytes already accepted by the kernel.

## Benchmark

```sh
python3 bench/run.py --build build --out results --pilot --rates 5000 30000 150000 --seconds 1
python3 bench/run.py --build build --out results --rates 5000 60000 150000 --seconds 1 --repeat 3 --perf
python3 -m pip install -r bench/requirements.txt
python3 bench/plot.py results
```

The matrix covers both queue modes, 1/4/16 subscribers, three offered rates, and steady or slow-last-subscriber traffic. Each condition gets a one-second warm-up trial using a fresh relay and three recorded trials. Slow subscribers use a small receive buffer and wait 20 ms before each read. In the single-subscriber slow case, the only subscriber is slow.

`runs.csv` contains publisher pacing, ingest, aggregate delivery, disconnect, queue, and peak process RSS measurements. `subscribers.csv` contains delivery rates, sequence gaps, and p50/p95/p99 latency for each subscriber. Latency is measured from frame creation before the publisher sends to the read that completes the frame. Rate denominators use the measured publisher interval, and delivery totals include the brief drain after publishing. A latency percentile is meaningful only for subscribers with deliveries. Aggregate delivery counts every copy of an update. `environment.json` records the host and benchmark configuration.

The benchmark uses a single shared Linux host and its loopback clock. Treat its results as measurements of this run and environment. Compare tail latency together with missed updates, disconnects, and achieved send rate, especially under overload. For another machine, rerun the pilot and choose rates around its measured saturation point.

## Sanitizers

```sh
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DPULSE_SANITIZER=address,undefined
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DPULSE_SANITIZER=thread
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

The tests exercise wire encoding, queue wraparound and concurrency, split and coalesced TCP frames, idle wakeups, upstream pauses, slow-client isolation, and the shutdown deadline. UDP ingress is a follow-on.

## Saturation profiling

```sh
python3 bench/run.py --build build --out results --profile-only --rates 5000 60000 150000 --seconds 3 --perf
```

This runs a separate 16-subscriber trial per queue. The JSON profiles report per-thread CPU time and voluntary and involuntary context switches from Linux procfs during publication. With `perf` installed and permitted, the command also captures task clock, context switches, cycles, and instructions. The bundled run used procfs because `perf` was unavailable.
