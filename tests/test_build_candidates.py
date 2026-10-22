import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CandidateTests(unittest.TestCase):
    def test_top_one_fallback_and_legacy_rebuild(self):
        script = Path(__file__).resolve().parents[1] / "src/build_candidates.py"
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / "events.csv", Path(directory) / "candidates.csv"
            with source.open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["timestamp", "visitorid", "event", "itemid", "transactionid"])
                for visitor, item in [(1, 10), (1, 20), (2, 10), (2, 20), (2, 30), (3, 40)]:
                    writer.writerow([1, visitor, "view", item, ""])
            command = [sys.executable, str(script), "--input", str(source), "--output", str(target)]
            subprocess.run(command, check=True, capture_output=True)
            original = target.read_bytes()
            with target.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 4)
            self.assertEqual(len({r["itemid"] for r in rows}), 4)
            by_item = {r["itemid"]: r for r in rows}
            self.assertEqual(by_item["10"]["recommended_itemid"], "20")
            self.assertEqual(by_item["10"]["candidate_score"], "2")
            self.assertEqual(by_item["40"]["method"], "popular_fallback")
            self.assertTrue(all(r["itemid"] != r["recommended_itemid"] for r in rows))
            with target.open("a") as stream:
                stream.write("10,30,1,co_view_topk\n")
            subprocess.run(command + ["--ensure-top-one"], check=True, capture_output=True)
            self.assertEqual(target.read_bytes(), original)
            result = subprocess.run(command + ["--ensure-top-one"], check=True, capture_output=True, text=True)
            self.assertIn("Reusing", result.stdout)


if __name__ == "__main__":
    unittest.main()
