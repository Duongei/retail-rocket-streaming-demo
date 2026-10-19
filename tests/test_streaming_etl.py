import os
import sys
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.streaming_etl import EVENT_SCHEMA, score_windows, transform_events


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

    def test_sliding_window_scores_intent_and_suppresses_purchased_item(self):
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
        self.assertFalse(item_two.high_intent)


if __name__ == "__main__":
    unittest.main()
