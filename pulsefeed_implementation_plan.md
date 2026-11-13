# PulseFeed implementation plan

## Project 5 from projects_context(3).md

PulseFeed is a hand-written C++ feed relay for Linux. It takes a stream of synthetic market-data updates and broadcasts them to subscribers. The project measures how concurrency, fanout, and slow consumers affect throughput, backpressure, and tail latency.

## MVP and stack

| Area | Choice |
|---|---|
| Build | C++20, CMake, GCC or Clang, Linux |
| Networking | Nonblocking TCP sockets and level-triggered `epoll` |
| Concurrency | One ingest thread, one fanout thread, and a bounded single-producer/single-consumer (SPSC) ring |
| Test tools | CTest, AddressSanitizer, UndefinedBehaviorSanitizer, and ThreadSanitizer in separate builds as needed |
| Benchmarks | C++ publisher and subscriber drivers, CSV results, Python plots, and Linux `perf stat` |

Start with one TCP publisher and multiple subscribers on a single Linux host. Use synthetic, numbered updates and broadcast every update to each subscriber. Add UDP ingress as a follow-on after the TCP benchmark works.

## Data path and overload behavior

1. **Publisher → ingest.** Define a fixed-size binary frame with a sequence number, instrument ID, integer price, quantity, and publisher timestamp. Specify byte order and encode fields explicitly. Handle fragmented and coalesced TCP reads and reject a connection if it closes with a partial frame.
2. **Ingest → fanout.** First prove the flow with a mutex-protected bounded queue. Then implement a preallocated SPSC ring and keep the mutex version as a comparison baseline. Use acquire/release ordering for publication and slot reuse. Wake the fanout loop with `eventfd` when work arrives and test the wakeup path around idle transitions.
3. **Fanout → subscribers.** Let the fanout thread own all subscriber sockets. Track partial writes per subscriber and bound each pending buffer. Enable write readiness only while data is pending. Give each writable socket a per-iteration byte budget so a busy subscriber cannot monopolize the loop.
4. **Backpressure.** When the ingest queue fills, retain any already-read frames in bounded staging and pause upstream reads until the queue drains below a low-water mark. Disconnect a subscriber that exceeds its pending-buffer limit and count the disconnect. Expose queue depth, high-water marks, pauses, and disconnects.
5. **Shutdown.** Stop accepting new connections, drain pending data up to a deadline, close sockets, and join both threads. Count any messages discarded at the deadline.

## Four-week schedule

| Week | Build | Exit check |
|---|---|---|
| 1 | Implement the frame codec, publisher, subscriber, and TCP relay for one consumer. | The subscriber receives numbered updates in order, including with split reads and partial writes. |
| 2 | Add multi-client fanout, bounded buffers, and upstream pause/resume. | A stalled subscriber hits its limit while healthy clients continue receiving ordered updates. Process memory stays bounded. |
| 3 | Add the SPSC ring, `eventfd` wakeups, and counters. Keep both queue implementations selectable at startup. | Wraparound, full/empty transitions, idle wakeups, and shutdown pass. Run concurrency stress tests under ThreadSanitizer. |
| 4 | Run the benchmark matrix, profile the saturated cases, and document the results. | A fresh Linux build can reproduce the demo and generate the benchmark CSV and plots. |

## Benchmark and acceptance criteria

- Sweep 1, 4, and 16 subscribers across three publish rates selected after a pilot to cover light load, near saturation, and overload. Test steady traffic and a deliberately slow subscriber. Warm up and repeat each measured condition three times.
- Report offered rate, actual publisher send rate, relay ingest rate, aggregate and per-subscriber delivery rates, p50/p95/p99 latency, queue high-water marks, and disconnects. Measure from the publisher timestamp to full-frame receipt using the same monotonic clock on one host. Record publisher pacing delays and sequence gaps so overload remains visible alongside latency percentiles.
- Compare the mutex queue and SPSC ring with identical frame sizes, queue capacities, and workloads. Record CPU, kernel, compiler flags, CPU affinity, and socket buffer settings. Label loopback measurements and document any optional two-host throughput run separately.
- Verify in-order delivery for healthy subscribers during stable runs, bounded memory during overload, isolation from stalled subscribers, and clean shutdown. Publish measured results with reproduction commands.

## Deliverables

C++ relay, publisher and subscriber drivers, tests, benchmark runner, CSV and plots, and a README with the protocol, build steps, overload policy, and demo commands. A follow-on UDP version can reuse the frame codec and add gap, duplicate, and reordering counters.
