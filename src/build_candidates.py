"""Build a small deterministic item-to-item co-view catalog from Retail Rocket CSV.

Run once before the demo. The source CSV is read only; output is a separate CSV.
The bounded scan is a reproducible classroom artifact, not a production model.
"""
import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/events_sorted.csv")
    parser.add_argument("--output", default="data/item_candidates.csv")
    parser.add_argument("--max-rows", type=int, default=500000)
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()
    visitors = defaultdict(set)
    all_items = set()
    source_rows = 0
    with Path(args.input).open(newline="", encoding="utf-8-sig") as source:
        for i, row in enumerate(csv.DictReader(source)):
            source_rows += 1
            all_items.add(row["itemid"])
            if i < args.max_rows and row["event"] in ("view", "addtocart"):
                visitors[row["visitorid"]].add(row["itemid"])
    pairs = Counter()
    popularity = Counter()
    for items in visitors.values():
        for item in items:
            popularity[item] += 1
        ordered = sorted(items)
        for item in ordered:
            pairs.update((item, other) for other in ordered if other != item)
    by_item = defaultdict(list)
    for (item, other), count in pairs.items():
        by_item[item].append((count, popularity[other], other))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerow(["itemid", "recommended_itemid", "candidate_score", "method"])
        for item, related in sorted(by_item.items()):
            for score, _, other in sorted(related, key=lambda x: (-x[0], -x[1], x[2]))[:args.top_k]:
                writer.writerow([item, other, score, "co_view_topk"])
        # Co-view evidence is bounded; every item in the source CSV still gets a
        # data-derived fallback when it has no co-view pair in that bounded prefix.
        most_popular = [item for item, _ in sorted(popularity.items(), key=lambda x: (-x[1], int(x[0]))) ]
        if most_popular:
            for item in sorted(all_items, key=int):
                if item not in by_item:
                    fallback = next((candidate for candidate in most_popular if candidate != item), None)
                    if fallback is not None:
                        writer.writerow([item, fallback, popularity[fallback], "popular_fallback"])
    print(f"Wrote {output}; scanned={source_rows} rows, co_view_prefix={args.max_rows}, "
          f"source_items={len(all_items)}, visitors={len(visitors)}")


if __name__ == "__main__":
    main()
