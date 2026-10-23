"""Read Retail Rocket CSV, replay events, and send JSON messages to Kafka."""
import argparse
import csv
from decimal import Decimal
import itertools
import json
import math
import os
from pathlib import Path
import random
import signal
import threading
import time

from kafka import KafkaProducer

# Default settings. Docker environment variables and CLI arguments can override them.
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "retail-rocket-events"
CSV_PATH = "data/events_sorted.csv"
LIMIT = None                      # None = whole file; 0 = send nothing
SKIP = 0                          # Manual row skip, not an automatic checkpoint
SPEEDUP = 20                       # 0 = fixed-rate mode
MAX_SLEEP = 5.0
RATE = 10
PROGRESS_EVERY = 100
SHUTDOWN_TIMEOUT = 15.0


def read_config(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)

    def option(name, env, default, value_type=str):
        parser.add_argument(name, type=value_type, default=os.getenv(env) or default)

    option("--broker", "KAFKA_BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS)
    option("--topic", "KAFKA_TOPIC", TOPIC)
    option("--csv-path", "CSV_PATH", CSV_PATH)
    option("--limit", "LIMIT", LIMIT, int)
    option("--skip", "SKIP", SKIP, int)
    option("--speedup", "SPEEDUP", SPEEDUP, float)
    option("--max-sleep", "MAX_SLEEP", MAX_SLEEP, float)
    option("--rate", "RATE", RATE, float)
    option("--progress-every", "PROGRESS_EVERY", PROGRESS_EVERY, int)
    option("--shutdown-timeout", "SHUTDOWN_TIMEOUT", SHUTDOWN_TIMEOUT, float)
    option("--reorder-buffer", "REORDER_BUFFER", 0, int)
    option("--reorder-seed", "REORDER_SEED", 7, int)
    config = parser.parse_args(argv)

    if (config.limit is not None and config.limit < 0) or config.skip < 0:
        parser.error("LIMIT and SKIP must be nonnegative")
    numbers = [config.speedup, config.max_sleep, config.rate, config.shutdown_timeout]
    if not all(math.isfinite(number) for number in numbers):
        parser.error("Numeric settings must be finite")
    if config.speedup < 0 or config.max_sleep < 0:
        parser.error("SPEEDUP and MAX_SLEEP must be nonnegative")
    if config.speedup == 0 and config.rate <= 0:
        parser.error("RATE must be positive in fixed-rate mode")
    if config.progress_every <= 0 or config.shutdown_timeout <= 0:
        parser.error("PROGRESS_EVERY and SHUTDOWN_TIMEOUT must be positive")
    if config.reorder_buffer < 0:
        parser.error("REORDER_BUFFER must be nonnegative")

    # The default data/ directory is at the repository root, above src/.
    config.csv_path = Path(__file__).resolve().parent.parent / config.csv_path
    return config


def make_event(row):
    transaction_id = None
    if row["transactionid"].strip():
        number = Decimal(row["transactionid"])
        if not number.is_finite() or number != number.to_integral_value():
            raise ValueError("transactionid must be an integer or empty")
        transaction_id = int(number)

    return {
        "timestamp": int(row["timestamp"]),
        "visitorid": int(row["visitorid"]),
        "event": row["event"],
        "itemid": int(row["itemid"]),
        "transactionid": transaction_id,
    }


def wait_before_send(event, previous_timestamp, config):
    if previous_timestamp is None:
        return
    if config.speedup > 0:
        gap = (event["timestamp"] - previous_timestamp) / 1000 / config.speedup
        time.sleep(min(max(gap, 0), config.max_sleep))
    else:
        time.sleep(1 / config.rate)


def reorder_rows(rows, buffer_size, seed):
    """Shuffle only bounded chunks, retaining every original timestamp and payload."""
    if not buffer_size:
        yield from rows
        return
    rng = random.Random(seed)
    buffer = []
    for row in rows:
        buffer.append(row)
        if len(buffer) == buffer_size:
            rng.shuffle(buffer)
            yield from buffer
            buffer = []
    rng.shuffle(buffer)
    yield from buffer


def print_progress(sent, stats, lock):
    with lock:
        confirmed, failed = stats["confirmed"], stats["failed"]
    unresolved = sent - confirmed - failed
    print(f"queued={sent} confirmed={confirmed} failed={failed} unresolved={unresolved}", flush=True)
    return failed, unresolved


def wait_for_results(producer, sent, stats, lock, deadline):
    # The same deadline covers flush, callback completion, and close.
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Shutdown deadline expired")
    producer.flush(timeout=remaining)
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("Timed out waiting for delivery results")
        with lock:
            if stats["confirmed"] + stats["failed"] == sent:
                return
        time.sleep(0.005)


def close_producer(producer, sent, stats, lock, timeout):
    deadline = time.monotonic() + timeout
    success = True
    try:
        wait_for_results(producer, sent, stats, lock, deadline)
    except (Exception, KeyboardInterrupt) as error:
        print(f"ERROR while waiting: {error}", flush=True)
        success = False
    finally:
        try:
            producer.close(timeout=max(0, deadline - time.monotonic()))
        except (Exception, KeyboardInterrupt) as error:
            print(f"ERROR while closing: {error}", flush=True)
            success = False
    return success


def main(argv=None):
    config = read_config(argv)
    # Each run owns its counters. Callbacks run on Kafka's background thread.
    stats = {"confirmed": 0, "failed": 0}
    lock = threading.Lock()

    def on_success(metadata):
        with lock:
            stats["confirmed"] += 1

    def on_error(error):
        with lock:
            stats["failed"] += 1
        print(f"Send failed: {error}", flush=True)

    producer = None
    sent = 0
    exit_code = 0
    previous_timestamp = None
    try:
        with config.csv_path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != ["timestamp", "visitorid", "event", "itemid", "transactionid"]:
                raise ValueError("CSV must contain the original five columns in order")
            if config.limit == 0:
                return 0

            producer = KafkaProducer(
                bootstrap_servers=config.broker,
                acks="all",
                enable_idempotence=True,
                max_in_flight_requests_per_connection=1,
                max_block_ms=10000,
                request_timeout_ms=10000,
                delivery_timeout_ms=30000,
            )
            rows = itertools.islice(reader, config.skip, None)
            rows = itertools.islice(rows, config.limit)
            indexed_rows = enumerate(rows, start=config.skip + 2)
            indexed_rows = reorder_rows(indexed_rows, config.reorder_buffer, config.reorder_seed)
            for line_number, row in indexed_rows:
                try:
                    event = make_event(row)
                except Exception as error:
                    raise ValueError(f"CSV line {line_number}: {error}") from error
                if previous_timestamp is not None and event["timestamp"] < previous_timestamp:
                    print(f"WARNING: timestamp decreased at line {line_number}; row retained", flush=True)

                wait_before_send(event, previous_timestamp, config)
                previous_timestamp = event["timestamp"]
                future = producer.send(
                    config.topic,
                    key=str(event["visitorid"]).encode("utf-8"),
                    value=json.dumps(event, allow_nan=False).encode("utf-8"),
                )
                sent += 1
                future.add_callback(on_success)
                future.add_errback(on_error)
                if sent % config.progress_every == 0:
                    print_progress(sent, stats, lock)
                with lock:
                    if stats["failed"]:
                        raise RuntimeError("Stopping after a delivery failure")

    except KeyboardInterrupt:
        print("Stopped; waiting for queued messages", flush=True)
        exit_code = 130
    except Exception as error:
        print(f"ERROR: {error}", flush=True)
        exit_code = 1
    finally:
        if producer is not None:
            if not close_producer(producer, sent, stats, lock, config.shutdown_timeout):
                exit_code = exit_code or 1
        failed, unresolved = print_progress(sent, stats, lock)
    if failed or unresolved:
        exit_code = exit_code or 1
    return exit_code


def stop_requested(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    raise SystemExit(main())
