"""Retail Rocket Kafka -> Spark Structured Streaming intent pipeline."""
import argparse
import json
import logging
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener

EVENT_SCHEMA = T.StructType([
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("visitorid", T.LongType(), True),
    T.StructField("event", T.StringType(), True),
    T.StructField("itemid", T.LongType(), True),
    T.StructField("transactionid", T.LongType(), True),
])
WEIGHTS = {"view": 1, "addtocart": 5, "transaction": 10}


class ProgressLogger(StreamingQueryListener):
    """Expose Kafka offset ranges and Spark's effective event-time watermark."""
    def __init__(self, output_path):
        super().__init__()
        self.offset_path = Path(output_path) / "intent_input_offsets"

    def onQueryStarted(self, event):
        logging.info("query_started id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event):
        logging.info("query_progress %s", event.progress.json)
        try:
            progress = json.loads(event.progress.json)
            if progress.get("name") != "retail-rocket-intent":
                return
            batch_id = progress["batchId"]
            offsets = {"batch_id": batch_id, "query_name": progress["name"],
                    "num_input_rows": progress.get("numInputRows"),
                    "sources": progress.get("sources", [])}
            self.offset_path.mkdir(parents=True, exist_ok=True)
            target = self.offset_path / f"batch_id={batch_id}.json"
            temporary = target.with_suffix(".tmp")
            temporary.write_text(json.dumps(offsets, ensure_ascii=False) + "\n", encoding="utf-8")
            temporary.replace(target)
        except Exception:
            logging.exception("Failed to save intent input offsets")

    def onQueryIdle(self, event):
        logging.info("query_idle id=%s", event.id)

    def onQueryTerminated(self, event):
        logging.info("query_terminated id=%s exception=%s", event.id, event.exception)


def read_config(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "retail-rocket-events"))
    parser.add_argument("--starting-offsets", default=os.getenv("STARTING_OFFSETS", "earliest"))
    parser.add_argument("--output", default=os.getenv("OUTPUT_PATH", "/app/output"))
    parser.add_argument("--checkpoint", default=os.getenv("CHECKPOINT_PATH", "/app/checkpoints/intent"))
    parser.add_argument("--session-checkpoint", default=os.getenv("SESSION_CHECKPOINT_PATH", "/app/checkpoints/sessions"))
    parser.add_argument("--console-checkpoint", default=os.getenv("CONSOLE_CHECKPOINT_PATH", "/app/checkpoints/console"))
    parser.add_argument("--raw-checkpoint", default=os.getenv("RAW_CHECKPOINT_PATH", "/app/checkpoints/raw_events"))
    parser.add_argument("--archive-raw-events", type=int, choices=(0, 1),
                        default=int(os.getenv("ARCHIVE_RAW_EVENTS", "1")))
    parser.add_argument("--session-gap", default=os.getenv("SESSION_GAP", "30 minutes"))
    parser.add_argument("--window-duration", default=os.getenv("WINDOW_DURATION", "10 minutes"))
    parser.add_argument("--slide-duration", default=os.getenv("SLIDE_DURATION", "5 minutes"))
    parser.add_argument("--watermark", default=os.getenv("WATERMARK", "5 minutes"))
    parser.add_argument("--trigger-seconds", type=int, default=int(os.getenv("TRIGGER_SECONDS", "5")))
    parser.add_argument("--intent-threshold", type=int, default=int(os.getenv("INTENT_THRESHOLD", "5")))
    parser.add_argument("--max-offsets-per-trigger", type=int,
                        default=int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "0")))
    parser.add_argument("--candidates", default=os.getenv("CANDIDATES_PATH", "/app/data/item_candidates.csv"))
    return parser.parse_args(argv)


def transform_events(kafka_df):
    """Parse Kafka JSON with explicit types and preserve the source event time."""
    return (kafka_df.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
            .select("e.*")
            .withColumn("event_time", F.timestamp_seconds(F.col("timestamp") / F.lit(1000.0)))
            .filter(F.col("timestamp").isNotNull() & F.col("visitorid").isNotNull()
                    & F.col("itemid").isNotNull() & F.col("event").isin(*WEIGHTS))
            .filter((F.col("event") != "transaction") | F.col("transactionid").isNotNull()))


def score_windows(events, window_duration="10 minutes", slide_duration="5 minutes",
                watermark="5 minutes", threshold=5):
    """Incremental event-time visitor/item windows; Update mode emits changed aggregates."""
    weighted = (events.withWatermark("event_time", watermark)
                .withColumn("engagement_score", F.when(F.col("event") == "view", 1)
                            .when(F.col("event") == "addtocart", 5).otherwise(10)))
    result = (weighted.groupBy(F.window("event_time", window_duration, slide_duration),
                            "visitorid", "itemid")
            .agg(F.sum("engagement_score").alias("intent_score"),
                F.sum(F.when(F.col("event") == "view", 1).otherwise(0)).alias("view_count"),
                F.sum(F.when(F.col("event") == "addtocart", 1).otherwise(0)).alias("addtocart_count"),
                F.sum(F.when(F.col("event") == "transaction", 1).otherwise(0)).alias("transaction_count"))
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .withColumn("event_date", F.to_date("window_start")))
    return result.withColumn("high_intent", (F.col("intent_score") >= threshold)
                            & (F.col("addtocart_count") > 0)
                            & (F.col("transaction_count") == 0))


def summarize_sessions(events, watermark="5 minutes", gap="30 minutes"):
    """Close visitor sessions after an inactivity gap, using event time."""
    grouped = (events.withWatermark("event_time", watermark)
            .groupBy("visitorid", F.session_window("event_time", gap))
            .agg(F.count("*").alias("event_count"),
                    F.sum(F.when(F.col("event") == "view", 1).otherwise(0)).alias("view_count"),
                    F.sum(F.when(F.col("event") == "addtocart", 1).otherwise(0)).alias("addtocart_count"),
                    F.sum(F.when(F.col("event") == "transaction", 1).otherwise(0)).alias("transaction_count"),
                    F.min("event_time").alias("first_event_time"),
                    F.max("event_time").alias("last_event_time")))
    return (grouped.withColumn("session_start", F.col("session_window.start"))
            .withColumn("session_end", F.col("session_window.end"))
            .withColumn("duration_seconds",
                        (F.unix_millis("last_event_time") - F.unix_millis("first_event_time")) / 1000.0)
            .withColumn("session_id", F.sha2(F.concat_ws(":", F.col("visitorid").cast("string"),
                                                        F.col("session_start").cast("string")), 256))
            .withColumn("event_date", F.to_date("session_start"))
            .drop("session_window"))


def main(argv=None):
    args = read_config(argv)
    if args.trigger_seconds <= 0 or args.max_offsets_per_trigger < 0:
        raise ValueError("trigger seconds must be positive and max offsets nonnegative")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (SparkSession.builder.appName("retail-rocket-intent")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "3"))
            .getOrCreate())
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.streams.addListener(ProgressLogger(args.output))
    source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.brokers)
            .option("subscribe", args.topic).option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "true"))
    if args.max_offsets_per_trigger:
        source = source.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
    raw = source.load()
    events = transform_events(raw)
    scored = score_windows(events, args.window_duration, args.slide_duration,
                        args.watermark, args.intent_threshold)
    sessions = summarize_sessions(events, args.watermark, args.session_gap)
    output = Path(args.output)
    (output / "intent").mkdir(parents=True, exist_ok=True)
    (output / "micro_batches").mkdir(parents=True, exist_ok=True)
    (output / "sessions").mkdir(parents=True, exist_ok=True)
    (output / "raw_events").mkdir(parents=True, exist_ok=True)
    # The small, offline-built item relation is static across micro-batches.
    candidate_path = Path(args.candidates)
    candidates = None
    if candidate_path.exists():
        candidates = (spark.read.option("header", True).csv(str(candidate_path))
                    .select(F.col("itemid").cast("long"), F.col("recommended_itemid").cast("long"),
                            F.col("candidate_score").cast("long"), F.col("method")))

    def write_batch(batch_df, batch_id):
        started = time.monotonic()
        batch_df.persist()
        try:
            changed_count = batch_df.count()
            # Durable append-by-batch purchase ledger prevents re-promoting an item
            # when its transaction falls in a different sliding window.
            ledger_root = output / "purchases"
            purchase_batch = batch_df.filter("transaction_count > 0").select("visitorid", "itemid").distinct()
            purchase_batch.write.mode("overwrite").parquet(str(ledger_root / f"batch_id={batch_id}"))
            ledger_parts = list(ledger_root.glob("batch_id=*"))
            if ledger_parts:
                purchases = spark.read.parquet(str(ledger_root / "batch_id=*"))\
                    .select("visitorid", "itemid").distinct()
                high = (batch_df.filter("high_intent").join(purchases, ["visitorid", "itemid"], "left_anti"))
            else:
                high = batch_df.filter("high_intent")
            if candidates is not None:
                recs = (high.alias("h").join(candidates.alias("c"),
                                            F.col("h.itemid") == F.col("c.itemid"), "inner")
                        .filter(F.col("h.itemid") != F.col("c.recommended_itemid"))
                        .select(F.col("h.visitorid").alias("visitorid"),
                                F.col("h.itemid").alias("interested_itemid"),
                                F.col("c.recommended_itemid").alias("recommended_itemid"),
                                F.col("h.intent_score").alias("intent_score"),
                                F.col("h.window_start").alias("window_start"),
                                F.col("h.window_end").alias("window_end"),
                                F.col("c.method").alias("method"),
                                F.col("c.candidate_score").alias("candidate_score")))
                # A candidate already bought by this visitor is not a useful recommendation.
                # Filter before ranking so the next eligible candidate can be selected.
                recs = recs.alias("r").join(
                    purchases.alias("p"),
                    (F.col("r.visitorid") == F.col("p.visitorid"))
                    & (F.col("r.recommended_itemid") == F.col("p.itemid")),
                    "left_anti",
                )
                # Candidate order is deterministic; emit at most one product per interested item/window.
                rank = Window.partitionBy("visitorid", "interested_itemid", "window_start", "window_end")\
                    .orderBy(F.col("candidate_score").desc(), F.col("recommended_itemid").asc())
                recs = (recs.withColumn("rn", F.row_number().over(rank)).filter("rn = 1")
                        .select("visitorid", "interested_itemid", "recommended_itemid",
                                "intent_score", "window_start", "window_end",
                                F.current_timestamp().alias("recommendation_time"),
                                F.lit(batch_id).alias("batch_id"), "method", "candidate_score"))
                recs = recs.dropDuplicates(["visitorid", "interested_itemid", "recommended_itemid"])
                history_root = output / "recommendation_history"
                history_parts = list(history_root.glob("batch_id=*") ) if history_root.exists() else []
                if history_parts:
                    sent_before = (spark.read.parquet(str(history_root / "batch_id=*"))
                                .filter(F.col("batch_id") != batch_id)
                                .select("visitorid", "interested_itemid", "recommended_itemid").distinct())
                    recs = recs.join(sent_before,
                                    ["visitorid", "interested_itemid", "recommended_itemid"], "left_anti")
                recs.show(100, truncate=False)
                recs.write.mode("overwrite").json(str(output / "recommendations" / f"batch_id={batch_id}"))
                recs.write.mode("overwrite").parquet(str(history_root / f"batch_id={batch_id}"))
            else:
                logging.warning("No candidate file %s; high-intent rows have no valid product recommendation", candidate_path)
                high.select("visitorid", "itemid", "intent_score", "window_start", "window_end").show(100, False)
            (batch_df.write.mode("overwrite").partitionBy("event_date")
            .parquet(str(output / "intent" / f"batch_id={batch_id}")))
            elapsed = round((time.monotonic() - started) * 1000, 2)
            metric = {"batch_id": batch_id, "aggregate_update_count": changed_count,
                      "duration_ms": elapsed, "completed_at_epoch_ms": int(time.time() * 1000)}
            logging.info("micro_batch %s", json.dumps(metric, sort_keys=True))
            (output / "micro_batches" / f"batch_id={batch_id}.json").write_text(
                json.dumps(metric) + "\n", encoding="utf-8")
        finally:
            batch_df.unpersist()

    query = (scored.writeStream.outputMode("update").foreachBatch(write_batch)
            .option("checkpointLocation", args.checkpoint).queryName("retail-rocket-intent")
            .trigger(processingTime=f"{args.trigger_seconds} seconds").start())
    def write_sessions(batch_df, batch_id):
        batch_df.persist()
        try:
            count = batch_df.count()
            if count:
                batch_df.show(100, truncate=False)
                (batch_df.write.mode("overwrite").partitionBy("event_date")
                .parquet(str(output / "sessions" / f"batch_id={batch_id}")))
            logging.info("session_batch batch_id=%s closed_sessions=%s", batch_id, count)
        finally:
            batch_df.unpersist()

    session_query = None
    console_query = None
    raw_query = None
    try:
        session_query = (sessions.writeStream.outputMode("append").foreachBatch(write_sessions)
                        .option("checkpointLocation", args.session_checkpoint)
                        .queryName("retail-rocket-sessions")
                        .trigger(processingTime=f"{args.trigger_seconds} seconds").start())
        console_query = (scored.select("window_start", "window_end", "visitorid", "itemid",
                                    "intent_score", "view_count", "addtocart_count",
                                    "transaction_count", "high_intent")
                        .writeStream.outputMode("update").format("console")
                        .option("truncate", "false").option("numRows", 30)
                        .option("checkpointLocation", args.console_checkpoint)
                        .queryName("retail-rocket-intent-console")
                        .trigger(processingTime=f"{args.trigger_seconds} seconds").start())
        raw_events = (raw.select(F.col("topic").alias("kafka_topic"),
                                F.col("partition").alias("kafka_partition"),
                                F.col("offset").alias("kafka_offset"),
                                F.col("timestamp").alias("kafka_timestamp"),
                                F.col("key").cast("string").alias("kafka_key"),
                                F.col("value").cast("string").alias("raw_json"))
                    .withColumn("parsed", F.from_json("raw_json", EVENT_SCHEMA))
                    .select("kafka_topic", "kafka_partition", "kafka_offset",
                            "kafka_timestamp", "kafka_key", "raw_json",
                            F.col("parsed.timestamp").alias("event_timestamp_ms"),
                            F.timestamp_seconds(F.col("parsed.timestamp") / F.lit(1000.0)).alias("event_time"),
                            F.col("parsed.visitorid").alias("visitorid"),
                            F.col("parsed.event").alias("event"),
                            F.col("parsed.itemid").alias("itemid"),
                            F.col("parsed.transactionid").alias("transactionid")))

        def write_raw_events(batch_df, batch_id):
            (batch_df.withColumn("batch_id", F.lit(batch_id))
            .write.mode("overwrite")
            .parquet(str(output / "raw_events" / f"batch_id={batch_id}")))

        if args.archive_raw_events:
            raw_query = (raw_events.writeStream.foreachBatch(write_raw_events)
                        .option("checkpointLocation", args.raw_checkpoint)
                        .queryName("retail-rocket-raw-events")
                        .trigger(processingTime=f"{args.trigger_seconds} seconds").start())
        logging.info("Started queries topic=%s brokers=%s trigger_seconds=%s", args.topic, args.brokers,
                    args.trigger_seconds)
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logging.info("Stopping streaming queries")
    finally:
        for active_query in (query, session_query, console_query, raw_query):
            if active_query is not None and active_query.isActive:
                active_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
