# Loopback benchmark results

108 measured trials and 756 subscriber records. Each condition ran three times after a separate warm-up. No subscriber reported out-of-order sequences. Queue and pending-buffer peaks stayed within their configured limits.

At an offered rate of 60,000 updates/s with 16 subscribers, both queue modes disconnected the deliberately slow subscriber in all three trials. The other 15 subscribers received every published update in those trials.

With 16 healthy subscribers and 150,000 updates/s offered, median achieved publish rates were about 90,490 updates/s for the SPSC mode and 149,999 for the mutex baseline. Median per-subscriber p99 latencies across those trials were about 219 ms and 5.2 ms, respectively. The queue mode comparison uses short loopback trials on a shared machine and captures the whole relay and its interaction with the publisher and subscribers. The results can vary with scheduling, hardware, and load.

The two thread profiles come from separate one-second saturation trials. The lower thread ID is the main ingress thread. The other thread handles fanout. See environment.json for the host and build configuration.
