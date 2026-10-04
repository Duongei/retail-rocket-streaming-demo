"""Replay Retail Rocket CSV without rewriting event timestamps."""
import argparse
import csv
from decimal import Decimal
import itertools
import json
import math
import os
import signal
from pathlib import Path
import threading
import time
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parent

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.queued = self.confirmed = self.failed = 0
    def queued_one(self):
        with self.lock:
            self.queued += 1
    def success(self, metadata):
        with self.lock:
            self.confirmed += 1
    def failure(self, error):
        with self.lock:
            self.failed += 1
        print(f'Delivery failed: {error!r}', flush=True)
    def snapshot(self):
        with self.lock:
            return self.queued, self.confirmed, self.failed, self.queued-self.confirmed-self.failed
    def report(self):
        q, c, f, u = self.snapshot()
        print(f'queued={q} confirmed={c} failed={f} unresolved={u}', flush=True)

def make_event(row):
    raw = row['transactionid'].strip()
    tx = Decimal(raw) if raw else None
    if tx is not None and (not tx.is_finite() or tx != tx.to_integral_value()):
        raise ValueError(f'Invalid integer transactionid: {raw}')
    return dict(timestamp=int(row['timestamp']), visitorid=int(row['visitorid']),
                event=row['event'], itemid=int(row['itemid']),
                transactionid=int(tx) if tx is not None else None)

def config(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    for flag, env, default in [('broker','KAFKA_BOOTSTRAP_SERVERS','localhost:9092'),
            ('topic','KAFKA_TOPIC','retail-rocket-events'), ('csv-path','CSV_PATH','data/events_sorted.csv')]:
        p.add_argument('--'+flag, default=os.getenv(env, default))
    for flag, env, default, typ in [('limit','LIMIT',None,int), ('skip','SKIP',0,int),
            ('speedup','SPEEDUP',20,float), ('max-sleep','MAX_SLEEP',5,float),
            ('rate','RATE',10,float), ('progress-every','PROGRESS_EVERY',100,int),
            ('shutdown-timeout','SHUTDOWN_TIMEOUT',15,float)]:
        p.add_argument('--'+flag, type=typ, default=os.getenv(env) or default)
    a = p.parse_args(argv)
    if (a.limit is not None and a.limit < 0) or a.skip < 0:
        p.error('limit/skip must be nonnegative')
    if any(not math.isfinite(v) for v in (a.speedup,a.max_sleep,a.rate,a.shutdown_timeout)):
        p.error('numeric settings must be finite')
    if a.speedup < 0 or a.max_sleep < 0 or a.progress_every <= 0 or a.shutdown_timeout <= 0:
        p.error('invalid speedup/max-sleep/progress-every/shutdown-timeout')
    if a.speedup == 0 and a.rate <= 0:
        p.error('fixed-rate requires rate > 0')
    a.csv_path = Path(a.csv_path)
    if not a.csv_path.is_absolute():
        a.csv_path = ROOT / a.csv_path
    return a

def wait_for_results(producer, stats, deadline):
    remaining = deadline-time.monotonic()
    if remaining <= 0:
        raise TimeoutError('Shutdown deadline expired before flush')
    producer.flush(timeout=remaining)
    while stats.snapshot()[3]:
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Delivery callbacks unresolved at deadline')
        time.sleep(min(0.005, remaining))
    if time.monotonic() > deadline:
        raise TimeoutError('Flush exceeded shutdown deadline')

def main(argv=None, producer_factory=KafkaProducer):
    a = config(argv)
    stats, producer, code = Stats(), None, 0
    previous = None
    try:
        with a.csv_path.open(encoding='utf-8-sig', newline='') as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != ['timestamp','visitorid','event','itemid','transactionid']:
                raise ValueError('CSV requires the original five columns in order')
            if a.limit != 0:
                producer = producer_factory(bootstrap_servers=a.broker, acks='all',
                    enable_idempotence=True, max_in_flight_requests_per_connection=1,
                    max_block_ms=10000, request_timeout_ms=10000, delivery_timeout_ms=30000)
                rows = itertools.islice(reader, a.skip, None if a.limit is None else a.skip+a.limit)
                for index, row in enumerate(rows, a.skip+2):
                    try:
                        event = make_event(row)
                    except Exception as exc:
                        raise ValueError(f'CSV line {index}: {exc}') from exc
                    if previous is not None and event['timestamp'] < previous:
                        print(f'WARNING: timestamp decreased at CSV line {index}; row retained', flush=True)
                    delay = (min(max(0,(event['timestamp']-previous)/1000/a.speedup),a.max_sleep)
                             if a.speedup and previous is not None else 0)
                    if not a.speedup and previous is not None:
                        delay = 1/a.rate
                    time.sleep(delay)
                    previous = event['timestamp']
                    future = producer.send(a.topic,key=str(event['visitorid']).encode('utf-8'),
                                           value=json.dumps(event,allow_nan=False).encode('utf-8'))
                    stats.queued_one()
                    future.add_callback(stats.success)
                    future.add_errback(stats.failure)
                    if stats.snapshot()[0] % a.progress_every == 0:
                        stats.report()
                    if stats.snapshot()[2]:
                        raise RuntimeError('Stopping after delivery failure')
    except KeyboardInterrupt:
        print('Interrupted; draining queued records', flush=True)
        code = 130
    except Exception as exc:
        print(f'ERROR: {exc!r}', flush=True)
        code = 1
    finally:
        if producer is not None:
            deadline = time.monotonic()+a.shutdown_timeout
            try:
                wait_for_results(producer,stats,deadline)
            except (Exception, KeyboardInterrupt) as exc:
                print(f'ERROR during drain: {exc!r}', flush=True)
                code = code or 1
            finally:
                try:
                    producer.close(timeout=max(0,deadline-time.monotonic()))
                except (Exception, KeyboardInterrupt) as exc:
                    print(f'ERROR during close: {exc!r}', flush=True)
                    code = code or 1
        stats.report()
    return code or (1 if any(stats.snapshot()[2:]) else 0)

if __name__ == '__main__':
    def stop_requested(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop_requested)
    raise SystemExit(main())

