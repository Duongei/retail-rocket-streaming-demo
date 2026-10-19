"""Show Kafka records consumed by one intent micro-batch using saved offset ranges."""

import argparse
import json
from functools import reduce
from operator import or_
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def parse_offsets(value):
    if value is None:
        return None
    return json.loads(value) if isinstance(value, str) else value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="/app/output")
    parser.add_argument("--intent-checkpoint", default="/app/checkpoints/intent")
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--visitorid", type=int)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if args.batch_id < 0 or args.limit <= 0:
        parser.error("batch-id must be nonnegative and limit must be positive")

    output = Path(args.output)
    manifest = output / "intent_input_offsets" / f"batch_id={args.batch_id}.json"
    if not manifest.exists():
        parser.error(f"No offset manifest at {manifest}; it is only created by new runs")
    progress = json.loads(manifest.read_text(encoding="utf-8"))
    ranges = []
    for source_index, source in enumerate(progress["sources"]):
        starts = parse_offsets(source.get("startOffset"))
        ends = parse_offsets(source.get("endOffset"))
        if not ends:
            continue
        if starts is None:
            initial = Path(args.intent_checkpoint) / "sources" / str(source_index) / "0"
            if not initial.exists():
                parser.error(f"No startOffset in progress or initial Kafka offset at {initial}")
            starts = json.loads(initial.read_text(encoding="utf-8").splitlines()[-1])
        for topic, partitions in ends.items():
            for partition, end in partitions.items():
                start = starts.get(topic, {}).get(partition)
                if start is None:
                    parser.error(f"Missing starting offset for {topic} partition {partition}")
                if int(end) > int(start):
                    ranges.append((topic, int(partition), int(start), int(end)))

    print(f"Intent batch {args.batch_id}: {progress.get('num_input_rows')} Kafka records according to Spark")
    if not ranges:
        print("No new Kafka offsets in this batch")
        return
    for topic, partition, start, end in ranges:
        print(f"  {topic}, partition {partition}: offsets [{start}, {end})")

    spark = SparkSession.builder.appName("inspect-intent-batch").getOrCreate()
    try:
        records = spark.read.parquet(str(output / "raw_events" / "batch_id=*"))
        conditions = [
            (F.col("kafka_topic") == topic)
            & (F.col("kafka_partition") == partition)
            & (F.col("kafka_offset") >= start)
            & (F.col("kafka_offset") < end)
            for topic, partition, start, end in ranges
        ]
        records = records.filter(reduce(or_, conditions))
        actual = records.count()
        print(f"Raw records archived for this intent batch: {actual}")
        expected = progress.get("num_input_rows")
        if expected is not None and actual != expected:
            print("WARNING: raw archive is incomplete or Kafka offsets are not fully available yet")
        if args.visitorid is not None:
            records = records.filter(F.col("visitorid") == args.visitorid)
        records.select(F.col("batch_id").alias("raw_batch_id"),
                       "kafka_topic", "kafka_partition", "kafka_offset",
                       "visitorid", "event", "itemid", "event_time", "kafka_timestamp", "transactionid",
                       "raw_json").orderBy("kafka_topic", "kafka_partition", "kafka_offset")\
            .show(args.limit, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
