import os
import sys
import unittest
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.streaming_etl import EVENT_SCHEMA, score_windows, transform_events, make_batch_writer


class StreamingEtlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("streaming-etl-tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_json_schema_and_timestamp_conversion(self):
        rows = [('{"timestamp":1430622004384,"visitorid":7,"event":"view",'
                 '"itemid":12,"transactionid":null}',)]
        kafka_rows = self.spark.createDataFrame(rows, ["value"])
        parsed = transform_events(kafka_rows).collect()
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].visitorid, 7)
        self.assertEqual(parsed[0].event_time.strftime("%Y-%m-%d %H:%M:%S"), "2015-05-03 03:00:04")

    def test_invalid_payload_and_unknown_event_are_dropped(self):
        rows = [
            ('{"timestamp":1,"visitorid":1,"event":"mystery","itemid":1}',),
            ("not-json",),
        ]
        parsed = transform_events(self.spark.createDataFrame(rows, ["value"]))
        self.assertEqual(parsed.count(), 0)

    def test_sliding_window_scores_intent_including_transactions(self):
        rows = [
            (1430622004384, 1, "view", 10, None),
            (1430622005384, 1, "addtocart", 10, None),
            (1430622006384, 2, "transaction", 20, 99),
        ]
        events = self.spark.createDataFrame(rows, EVENT_SCHEMA).withColumn(
            "event_time", (F.col("timestamp").cast("double") / 1000).cast("timestamp"))
        scored = score_windows(events, "10 minutes", "5 minutes").collect()
        item_one = next(row for row in scored if row.visitorid == 1)
        item_two = next(row for row in scored if row.visitorid == 2)
        self.assertEqual(item_one.intent_score, 6)
        self.assertTrue(item_one.high_intent)
        self.assertEqual(item_one.view_count, 1)
        self.assertEqual(item_one.addtocart_count, 1)
        self.assertEqual(item_one.transaction_count, 0)
        self.assertEqual(item_two.intent_score, 10)
        self.assertTrue(item_two.high_intent)

    def test_threshold_only_views_and_custom_threshold(self):
        rows = [(1430622004384 + i, 1, "view", 10, None) for i in range(5)]
        events = self.spark.createDataFrame(rows, EVENT_SCHEMA).withColumn(
            "event_time", F.timestamp_seconds(F.col("timestamp") / 1000))
        self.assertTrue(all(r.high_intent for r in score_windows(events).collect()))
        self.assertFalse(any(r.high_intent for r in score_windows(events, threshold=6).collect()))

    def test_recommendation_history_across_batches_retry_and_restart(self):
        candidates = self.spark.createDataFrame([(10, 20, 9, "co_view_topk")],
            ["itemid", "recommended_itemid", "candidate_score", "method"])

        def scored_rows(views, purchased=False, later=False):
            timestamp = 1430622004384 + (3600000 if later else 0)
            rows = [(timestamp + i, 1, "view", 10, None) for i in range(views)]
            if purchased:
                # Both the source item and candidate are bought; neither blocks recommendation.
                rows += [(timestamp, 1, "transaction", 10, 99),
                         (timestamp, 1, "transaction", 20, 100)]
            events = self.spark.createDataFrame(rows, EVENT_SCHEMA).withColumn(
                "event_time", F.timestamp_seconds(F.col("timestamp") / 1000))
            return score_windows(events)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "micro_batches").mkdir()
            writer = make_batch_writer(self.spark, output, candidates, "fixture.csv")
            writer(scored_rows(4), 0)
            self.assertEqual(self.spark.read.parquet(str(output / "recommendation_history/batch_id=0")).count(), 0)
            writer(scored_rows(5, purchased=True), 1)
            path = str(output / "recommendation_history/batch_id=1")
            first = self.spark.read.parquet(path).collect()
            self.assertEqual(len(first), 1)  # overlapping windows emit only one recommendation
            self.assertEqual((first[0].visitorid, first[0].interested_itemid, first[0].recommended_itemid), (1, 10, 20))
            writer(scored_rows(5, purchased=True), 1)  # retry the same batch
            self.assertEqual(self.spark.read.parquet(path).count(), 1)
            # Recreate the callback to ensure dedup uses disk history, not process memory.
            writer = make_batch_writer(self.spark, output, candidates, "fixture.csv")
            writer(scored_rows(8, later=True), 2)
            self.assertEqual(self.spark.read.parquet(str(output / "recommendation_history/batch_id=2")).count(), 0)
            self.assertTrue(all(r.intent_score == 8 for r in self.spark.read.parquet(str(output / "intent/batch_id=2")).collect()))
            self.assertEqual(self.spark.read.json(str(output / "recommendations/batch_id=1")).count(), 1)
            self.assertFalse((output / "purchases").exists())


if __name__ == "__main__":
    unittest.main()
