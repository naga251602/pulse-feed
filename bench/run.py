#!/usr/bin/env python3
import argparse
import csv
import itertools
import json
import os
import platform
import select
import shutil
import signal
import socket
import subprocess
import time
from contextlib import ExitStack
from pathlib import Path


def ports():
    with socket.socket() as first, socket.socket() as second:
        for sock in (first, second):
            sock.bind(("127.0.0.1", 0))
        return first.getsockname()[1], second.getsockname()[1]


def ready(process):
    if select.select([process.stdout], [], [], 8)[0]:
        if process.stdout.readline().strip() == "READY":
            return
    raise RuntimeError(f"{process.args} failed to become ready")


def child(stack, args):
    process = stack.enter_context(
        subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    )
    stack.callback(lambda: process.kill() if process.poll() is None else None)
    return process


def result(process, timeout):
    out, err = process.communicate(timeout=timeout)
    if process.returncode:
        raise RuntimeError(f"{process.args}: {err}")
    return json.loads(out)


def rss_kib(pid):
    with Path(f"/proc/{pid}/status").open() as status:
        return int(next(line for line in status if line.startswith("VmHWM:")).split()[1])


def thread_counters(pid):
    counters = {}
    ticks = os.sysconf("SC_CLK_TCK")
    for thread in Path(f"/proc/{pid}/task").iterdir():
        fields = (thread / "stat").read_text().rsplit(")", 1)[1].split()
        switches = {
            key: int(value) for line in (thread / "status").read_text().splitlines()
            for key, _, value in [line.partition(":")]
            if key in {"voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"}
        }
        counters[thread.name] = dict(cpu_seconds=(int(fields[11]) + int(fields[12])) / ticks,
                                     **switches)
    return counters


def trial(args, mode, count, rate, slow, seconds, profile=None, software_profile=None):
    ingress, egress = ports()
    with ExitStack() as stack:
        relay = child(stack, [
            args.build / "pulsefeed", "--ingress", str(ingress), "--egress", str(egress),
            "--queue", mode, "--queue-frames", "4096", "--pending-frames", "4096",
            "--socket-buffer", "16384",
        ])
        ready(relay)
        profiler = None
        if profile:
            profiler = child(stack, [
                "perf", "stat", "-e", "task-clock,context-switches,cycles,instructions",
                "-o", str(profile), "-p", str(relay.pid),
            ])
        subscribers = []
        for index in range(count):
            subscriber = child(stack, [args.build / "subscriber", str(egress),
                                       "20" if slow and index == count - 1 else "0"])
            ready(subscriber)
            subscribers.append(subscriber)
        time.sleep(0.025)
        baseline = thread_counters(relay.pid) if software_profile else None
        publisher = child(stack, [args.build / "publisher", str(ingress), str(rate), str(seconds)])
        sent = result(publisher, seconds + 5)
        if baseline is not None:
            final = thread_counters(relay.pid)
            deltas = {tid: {key: value - baseline[tid][key] for key, value in row.items()}
                      for tid, row in final.items()}
            software_profile.write_text(json.dumps(
                dict(mode=mode, subscribers=count, offered_rate=rate,
                     publish_seconds=sent["publish_seconds"], clock_ticks=os.sysconf("SC_CLK_TCK"),
                     threads=deltas), indent=2) + "\n")
        # Allow the relay to read any remaining bytes in the publisher socket.
        time.sleep(0.06)
        peak_rss = rss_kib(relay.pid)
        relay.send_signal(signal.SIGTERM)
        stats = result(relay, 4)
        if profiler:
            _, error = profiler.communicate(timeout=5)
            if profiler.returncode:
                profile.write_text(error)
        deliveries = [result(subscriber, 8) for subscriber in subscribers]
    elapsed = sent["publish_seconds"]
    row = dict(mode=mode, subscribers=count, slow_last=slow, offered_rate=rate,
               actual_send_rate=sent["sent"] / elapsed,
               ingested_per_publish_second=stats["ingested"] / elapsed,
               delivered_per_publish_second=sum(item["received"] for item in deliveries) / elapsed,
               peak_rss_kib=peak_rss, **sent, **stats)
    per_client = [
        dict(index=i, slow=slow and i == count - 1,
             delivery_rate=item["received"] / elapsed,
             missing_from_run=sent["sent"] - item["received"], **item)
        for i, item in enumerate(deliveries)
    ]
    return row, per_client


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=Path("results"))
    parser.add_argument("--rates", type=int, nargs=3, default=[2000, 20000, 100000])
    parser.add_argument("--seconds", type=int, default=2)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--perf", action="store_true")
    parser.add_argument("--profile-only", action="store_true")
    args = parser.parse_args()
    args.build = args.build.resolve()
    if args.seconds < 1 or args.repeat < 1 or not all(0 < x <= 1000000 for x in args.rates):
        parser.error("invalid duration, repeat count, or rate")
    args.out.mkdir(parents=True, exist_ok=True)
    if args.profile_only:
        for mode in ("spsc", "mutex"):
            trial(args, mode, 16, max(args.rates), False, args.seconds,
                  profile=(args.out / f"perf_{mode}.txt") if args.perf and shutil.which("perf") else None,
                  software_profile=args.out / f"thread_profile_{mode}.json")
        return
    rows, clients = [], []
    if args.pilot:
        matrix = itertools.product(["spsc"], [16], args.rates, [False], [1])
    else:
        matrix = itertools.product(["spsc", "mutex"], [1, 4, 16], args.rates,
                                   [False, True], range(1, args.repeat + 1))
    warmed = set()
    for mode, count, rate, slow, repeat in matrix:
        key = mode, count, rate, slow
        if key not in warmed:
            trial(args, *key, 1)
            warmed.add(key)
        row, per_client = trial(args, *key, args.seconds)
        labels = dict(run=len(rows) + 1, repeat=repeat, mode=mode, subscribers=count,
                      slow_last=slow, offered_rate=rate)
        rows.append(dict(labels, **{k: v for k, v in row.items() if k not in labels}))
        clients.extend(dict(labels, **item) for item in per_client)
        print(labels, "sent", row["sent"], "disconnects", row["backpressure_disconnects"], flush=True)
    prefix = "pilot_" if args.pilot else ""
    write_csv(args.out / f"{prefix}runs.csv", rows)
    write_csv(args.out / f"{prefix}subscribers.csv", clients)
    if args.perf and not args.pilot:
        if shutil.which("perf"):
            for mode in ("spsc", "mutex"):
                trial(args, mode, 16, max(args.rates), False, args.seconds, args.out / f"perf_{mode}.txt")
        else:
            (args.out / "perf_unavailable.txt").write_text("Linux perf was unavailable on this host.\n")
    cache = args.build / "CMakeCache.txt"
    cache_entries = dict(line.split("=", 1) for line in cache.read_text().splitlines()
                         if line.startswith(("CMAKE_CXX_COMPILER:", "CMAKE_CXX_FLAGS:",
                                             "CMAKE_CXX_FLAGS_RELEASE:", "CMAKE_BUILD_TYPE:"))) if cache.exists() else {}
    cpuinfo = Path("/proc/cpuinfo").read_text()
    cpu = next((line.partition(":")[2].strip() for line in cpuinfo.splitlines()
                if line.startswith("model name")), platform.machine())
    environment = dict(cpu=cpu, kernel=platform.release(), transport="TCP IPv4 loopback",
                       affinity=sorted(os.sched_getaffinity(0)), relay_socket_buffer_request=16384,
                       rates=args.rates, seconds=args.seconds, repeat=args.repeat, build=cache_entries)
    (args.out / f"{prefix}environment.json").write_text(json.dumps(environment, indent=2) + "\n")


if __name__ == "__main__":
    main()
