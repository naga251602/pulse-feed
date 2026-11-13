#!/usr/bin/env python3
import argparse
import csv
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    runs = read_csv(args.directory / "runs.csv")
    clients = read_csv(args.directory / "subscribers.csv")
    fig, axes = plt.subplots(2, 3, figsize=(12, 6), layout="constrained")
    for column, count in enumerate(("1", "4", "16")):
        for mode in ("spsc", "mutex"):
            selected = [r for r in runs if r["subscribers"] == count and r["mode"] == mode
                        and r["slow_last"] == "False"]
            rates = sorted({int(r["offered_rate"]) for r in selected})
            delivery, p99 = [], []
            for rate in rates:
                matches = [r for r in selected if int(r["offered_rate"]) == rate]
                delivery.append(statistics.median(float(r["delivered_per_publish_second"])
                                                  for r in matches) / 1000)
                run_ids = {r["run"] for r in matches}
                samples = [float(c["p99_ns"]) / 1000 for c in clients
                           if c["run"] in run_ids and int(c["received"])]
                p99.append(statistics.median(samples) if samples else float("nan"))
            for row, metric in enumerate((delivery, p99)):
                axes[row, column].plot([rate / 1000 for rate in rates], metric, "o-", label=mode)
                axes[row, column].grid(alpha=0.25)
        axes[0, column].set_title(f"{count} subscriber(s)")
        axes[1, column].set_xlabel("Offered rate (k updates/s)")
    axes[0, 0].set_ylabel("Aggregate delivery (k updates/s)")
    axes[1, 0].set_ylabel("Median subscriber p99 (µs)")
    axes[0, 0].legend()
    fig.suptitle("PulseFeed TCP loopback · all subscribers reading")
    fig.savefig(args.directory / "loopback.png", dpi=150)
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(10, 4), layout="constrained")
    for axis, mode in zip(axes, ("spsc", "mutex")):
        for slow in ("False", "True"):
            selected = [c for c in clients if c["mode"] == mode and c["subscribers"] == "16"
                        and c["slow_last"] == slow and c["slow"] == "False"]
            rates = sorted({int(c["offered_rate"]) for c in selected})
            p99 = [statistics.median(float(c["p99_ns"]) / 1000 for c in selected
                                     if int(c["offered_rate"]) == rate and int(c["received"]))
                   if any(int(c["offered_rate"]) == rate and int(c["received"]) for c in selected)
                   else float("nan") for rate in rates]
            axis.plot([rate / 1000 for rate in rates], p99, "o-",
                      label="One slow subscriber" if slow == "True" else "All reading")
        axis.set_title(mode)
        axis.set_xlabel("Offered rate (k updates/s)")
        axis.grid(alpha=0.25)
    axes[0].set_ylabel("Median healthy subscriber p99 (µs)")
    axes[0].legend()
    fig.suptitle("PulseFeed slow-subscriber isolation · 16 subscribers")
    fig.savefig(args.directory / "slow_subscriber.png", dpi=150)


if __name__ == "__main__":
    main()
