"""Offer a reproducible workload and save per-request HTTP results and sampled terminal latency."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from http.client import HTTPConnection, HTTPException, HTTPSConnection
import json
import math
from pathlib import Path
from threading import local
import time
from urllib.parse import urlsplit
from uuid import uuid4


connections = local()


def request(base, path, user, timeout, key=None):
    destination = urlsplit(base + path)
    address = (destination.scheme, destination.hostname, destination.port, timeout)
    if getattr(connections, "address", None) != address:
        old = getattr(connections, "client", None)
        if old is not None:
            old.close()
        connection_type = HTTPSConnection if destination.scheme == "https" else HTTPConnection
        connections.client = connection_type(destination.hostname, destination.port, timeout=timeout)
        connections.address = address

    client = connections.client
    headers = {"X-User-Id": str(user)}
    if key is not None:
        headers["Idempotency-Key"] = key
    target = destination.path or "/"
    if destination.query:
        target += "?" + destination.query
    try:
        client.request("GET" if key is None else "POST", target,
                       body=None if key is None else b"", headers=headers)
        with client.getresponse() as reply:
            body = reply.read()
            status, location = reply.status, reply.getheader("Location")
    except (HTTPException, OSError):
        client.close()
        raise
    try:
        document = json.loads(body) if body else {}
    except (ValueError, UnicodeError):
        document = {}
    return status, location, document if isinstance(document, dict) else {}


def observe(args, location, user, started):
    until = started + args.terminal_timeout
    while time.perf_counter() < until:
        try:
            code, _, body = request(args.base_url, location, user, args.http_timeout)
        except (HTTPException, OSError):
            pass
        else:
            status = body.get("status")
            if code == 200 and status in {"ORDER_CREATED", "REJECTED", "SYSTEM_FAILED"}:
                return status, body.get("reason"), (time.perf_counter() - started) * 1000
        time.sleep(args.poll_interval)
    return "OBSERVATION_TIMEOUT", None, None


def submit(args, index, offered, origin, polls):
    started = time.perf_counter()
    user = args.first_user + index
    key = f"{args.run_key}-{index}"
    sampled = args.observe_every > 0 and index % args.observe_every == 0
    try:
        code, location, body = request(
            args.base_url, f"/campaigns/{args.campaign}/purchase-requests", user, args.http_timeout, key)
    except (HTTPException, OSError, ValueError) as error:
        code, location, body = 0, None, {"status": "CLIENT_ERROR", "reason": type(error).__name__}
    ended = time.perf_counter()
    status = body.get("status")
    reason = body.get("reason")
    poll = None
    terminal_ms = None
    if sampled and code == 202 and location and location.startswith("/purchase-requests/"):
        poll = polls.submit(observe, args, location, user, started)
    elif sampled and status in {"ORDER_CREATED", "REJECTED", "SYSTEM_FAILED"}:
        terminal_ms = (ended - started) * 1000
    row = {
        "index": index, "user_id": user, "key": key,
        "offered_ms": (offered - origin) * 1000,
        "started_ms": (started - origin) * 1000,
        "post_ms": (ended - started) * 1000,
        "post_end_ms": (ended - origin) * 1000,
        "http_status": code, "post_status": status, "post_reason": reason,
        "request_uid": body.get("requestUid"),
        "sampled": sampled,
        "terminal_status": status if terminal_ms is not None else None,
        "terminal_reason": reason if terminal_ms is not None else None,
        "terminal_ms": terminal_ms,
    }
    return row, poll


def percentiles(values):
    ordered = sorted(values)
    return {f"p{p}": ordered[math.ceil(p / 100 * len(ordered)) - 1] if ordered else None
            for p in (50, 95, 99)}


def run(args):
    with (ThreadPoolExecutor(max_workers=args.workers) as senders,
          ThreadPoolExecutor(max_workers=args.poll_workers) as polls):
        origin = time.perf_counter()
        submissions = []
        for index in range(args.requests):
            offered = origin + index / args.rate if args.rate else origin
            wait = offered - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
            submissions.append(senders.submit(submit, args, index, offered, origin, polls))

        rows = []
        for submission in submissions:
            row, poll = submission.result()
            if poll is not None:
                row["terminal_status"], row["terminal_reason"], row["terminal_ms"] = poll.result()
            rows.append(row)

    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / "requests.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    post_window = max(row["post_end_ms"] for row in rows) / 1000
    successful = [row for row in rows if row["http_status"] in (201, 202)]
    summary = {
        "run": args.run_key, "campaign": args.campaign, "offered_count": len(rows),
        "target_per_second": args.rate, "workers": args.workers, "poll_workers": args.poll_workers,
        "poll_interval_s": args.poll_interval, "observe_every": args.observe_every,
        "http_counts": dict(Counter(row["http_status"] for row in rows)),
        "status_counts": dict(Counter(row["post_status"] or "UNKNOWN" for row in rows)),
        "reason_counts": dict(Counter(row["post_reason"] or "NONE" for row in rows)),
        "post_window_s": post_window, "successful_posts": len(successful),
        "successful_posts_per_second": len(successful) / post_window,
        "post_ms": percentiles(row["post_ms"] for row in rows if row["http_status"]),
        "successful_post_ms": percentiles(row["post_ms"] for row in successful),
        "send_delay_ms": percentiles(row["started_ms"] - row["offered_ms"] for row in rows),
        "terminal_sample_counts": dict(Counter(
            row["terminal_status"] for row in rows if row["terminal_status"] is not None)),
        "order_created_observed_ms": percentiles(
            row["terminal_ms"] for row in rows if row["terminal_status"] == "ORDER_CREATED"),
    }
    (args.out / "http-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--campaign", required=True, type=int)
    parser.add_argument("--requests", required=True, type=int)
    parser.add_argument("--rate", type=float, help="Offered requests/s. Omit for an immediate burst.")
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--first-user", type=int, default=10_000)
    parser.add_argument("--observe-every", type=int, default=10, help="Sample every Nth request. 0 disables observation.")
    parser.add_argument("--poll-workers", type=int, default=16)
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument("--http-timeout", type=float, default=10)
    parser.add_argument("--terminal-timeout", type=float, default=60)
    parser.add_argument("--run-key", default=uuid4().hex[:12])
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.base_url = args.base_url.rstrip("/")
    if urlsplit(args.base_url).scheme not in {"http", "https"} or not urlsplit(args.base_url).hostname:
        parser.error("--base-url must be an HTTP or HTTPS URL")
    timeouts = (args.poll_interval, args.http_timeout, args.terminal_timeout)
    if (min(args.requests, args.campaign, args.first_user, args.workers, args.poll_workers) <= 0
            or any(not math.isfinite(value) or value <= 0 for value in timeouts)
            or args.rate is not None and (not math.isfinite(args.rate) or args.rate <= 0)
            or args.observe_every < 0 or not args.run_key.isascii() or not args.run_key.isalnum()
            or len(args.run_key) > 96):
        parser.error("Use positive load and timeout settings and an alphanumeric run key up to 96 characters.")
    if (args.out / "requests.csv").exists() or (args.out / "http-summary.json").exists():
        parser.error("Choose a new --out directory to preserve earlier results.")
    run(args)


if __name__ == "__main__":
    main()
