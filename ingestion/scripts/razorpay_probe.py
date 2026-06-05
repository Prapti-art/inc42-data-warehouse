"""Razorpay API probe — fetches last N payments and dumps their structure.

Goal: understand how course payments are identified in YOUR Razorpay account
(notes.course_name? payment_page_id? description prefix?) BEFORE we write
the full ingest. Pulls everything — does not filter, does not load to BQ.

Run locally:
  export RZP_KEY_ID='rzp_live_...'
  export RZP_KEY_SECRET='...'
  python3 ingestion/scripts/razorpay_probe.py [--count 20] [--days-back 60]
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta

import requests

API = "https://api.razorpay.com/v1"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--days-back", type=int, default=60)
    args = parser.parse_args()

    key_id = os.environ.get("RZP_KEY_ID")
    key_secret = os.environ.get("RZP_KEY_SECRET")
    if not key_id or not key_secret:
        sys.exit("Set RZP_KEY_ID and RZP_KEY_SECRET env vars first.")

    from_ts = int((datetime.utcnow() - timedelta(days=args.days_back)).timestamp())

    r = requests.get(
        f"{API}/payments",
        auth=(key_id, key_secret),
        params={"from": from_ts, "count": args.count},
        timeout=30,
    )
    if r.status_code != 200:
        sys.exit(f"Razorpay error {r.status_code}: {r.text[:500]}")

    data = r.json()
    items = data.get("items", [])
    print(f"Pulled {len(items)} payments from last {args.days_back} days "
          f"(account mode: {'LIVE' if key_id.startswith('rzp_live_') else 'TEST'})\n")

    # Summary of what's populated
    print("─" * 70)
    print("FIELD POPULATION across payments:")
    print("─" * 70)
    field_pop = Counter()
    notes_keys = Counter()
    desc_samples = []
    for p in items:
        for k, v in p.items():
            if v not in (None, "", [], {}):
                field_pop[k] += 1
        if p.get("notes"):
            for nk in p["notes"]:
                notes_keys[nk] += 1
        if p.get("description"):
            desc_samples.append(p["description"][:80])

    for k, c in field_pop.most_common(40):
        print(f"  {k:25s} {c}/{len(items)}")

    print("\n" + "─" * 70)
    print("notes KEYS (these tell us how each payment is tagged):")
    print("─" * 70)
    if not notes_keys:
        print("  (no notes set on any payment — so we'll need to use")
        print("   description / payment_page_id / order_id to identify courses)")
    for k, c in notes_keys.most_common():
        print(f"  notes.{k:25s} {c}/{len(items)}")

    print("\n" + "─" * 70)
    print("description samples (last 10):")
    print("─" * 70)
    for d in desc_samples[:10]:
        print(f"  {d}")

    print("\n" + "─" * 70)
    print("Full sample (most recent payment):")
    print("─" * 70)
    if items:
        print(json.dumps(items[0], indent=2, default=str))


if __name__ == "__main__":
    main()
