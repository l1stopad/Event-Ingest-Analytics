import csv
import json
import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import argparse
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100_000, help="кількість подій")
    ap.add_argument("--out", type=Path, default=Path("data/bench_100k.csv"))
    ap.add_argument("--start", type=str, default="2025-08-01", help="start date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=30, help="скільки днів розкидати")
    ap.add_argument("--users", type=int, default=10_000, help="кількість унікальних користувачів")
    args = ap.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    start_date = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    event_types = ["signin", "view", "click", "purchase"]

    with args.out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "occurred_at", "user_id", "event_type", "properties_json"])

        for i in range(args.n):
            d = start_date + timedelta(days=random.randint(0, args.days - 1),
                                       hours=random.randint(0, 23),
                                       minutes=random.randint(0, 59),
                                       seconds=random.randint(0, 59))
            user_id = f"u{random.randint(1, args.users)}"
            et = random.choice(event_types)
            country = random.choice(["UA", "PL", "DE", "US", "GB"])
            props = {"country": country, "rand": random.randint(0, 999)}
            w.writerow([str(uuid4()), d.isoformat(), user_id, et, json.dumps(props)])

    print(f"[OK] wrote {args.n} rows → {args.out}")

if __name__ == "__main__":
    main()
