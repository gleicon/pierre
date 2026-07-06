#!/usr/bin/env python3
"""Synthetic log generator for the end-to-end Promtail->Pierre test.

Appends realistic JSON log lines to a file over a fixed duration, at a steady
rate, so Promtail (tailing the file) has to actually do real-time shipping,
not just a one-shot batch read. Each line carries a unique run marker so the
test can verify exact content made it through Promtail -> Pierre, not just a
plausible-looking count.
"""
import json
import random
import sys
import time

LEVELS = ["info", "info", "info", "warn", "error"]
MESSAGES = [
    "user login succeeded",
    "payment gateway timeout",
    "cache miss for key",
    "database connection pool exhausted",
    "request completed",
    "retrying upstream call",
]


def main():
    path = sys.argv[1]
    marker = sys.argv[2]
    count = int(sys.argv[3])
    duration_secs = float(sys.argv[4])

    interval = duration_secs / count if count else 0
    with open(path, "a") as f:
        for i in range(count):
            line = {
                "level": random.choice(LEVELS),
                # marker is a standalone token identical across every line (reliable
                # single BM25 term to search for); seq is a separate token unique per
                # line (used to verify every line made it through, none dropped/dup'd).
                "msg": f"{random.choice(MESSAGES)} runmarker {marker} seq {i:05d}",
            }
            f.write(json.dumps(line) + "\n")
            f.flush()
            time.sleep(interval)

    print(f"generated {count} lines with marker {marker} over {duration_secs}s", file=sys.stderr)


if __name__ == "__main__":
    main()
