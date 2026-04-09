"""
Gender detection for 20K names using Claude API.
Batches names (200 per call) to minimize API cost and latency.
"""

import anthropic
import csv
import json
import time
import os

# ── CONFIG ──
INPUT_CSV = "names_input.csv"       # columns: id, first_name
OUTPUT_CSV = "names_with_gender.csv" # columns: id, first_name, gender, confidence
BATCH_SIZE = 200                     # names per API call (~100 API calls for 20K)
MODEL = "claude-sonnet-4-6-20250514"

client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env


def detect_gender_batch(names: list[dict]) -> list[dict]:
    """Send a batch of names to Claude and get gender predictions."""

    names_text = "\n".join(f"{n['id']}|{n['first_name']}" for n in names)

    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": f"""Determine the gender for each person based on their first name.
These are primarily Indian names but may include other origins.

Return ONLY a JSON array with objects having these fields:
- id: the person's id
- gender: "male", "female", or "unknown"
- confidence: "high", "medium", or "low"

Names (format: id|first_name):
{names_text}"""
            }
        ],
    )

    # Parse JSON from response
    text = response.content[0].text
    # Handle cases where Claude wraps in ```json blocks
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]

    return json.loads(text.strip())


def main():
    # ── SAMPLE DATA (replace with your CSV read for real data) ──
    sample_names = [
        {"id": 1, "first_name": "Priya"},
        {"id": 2, "first_name": "Rahul"},
        {"id": 3, "first_name": "Neha"},
        {"id": 4, "first_name": "Amit"},
        {"id": 5, "first_name": "Vikram"},
        {"id": 6, "first_name": "Anjali"},
        {"id": 7, "first_name": "Siddharth"},
        {"id": 8, "first_name": "Kavitha"},
        {"id": 9, "first_name": "Arjun"},
        {"id": 10, "first_name": "Deepika"},
        {"id": 11, "first_name": "Manish"},
        {"id": 12, "first_name": "Pooja"},
        {"id": 13, "first_name": "Kiran"},       # unisex
        {"id": 14, "first_name": "Nikita"},       # unisex in India
        {"id": 15, "first_name": "Harpreet"},     # unisex
        {"id": 16, "first_name": "Suresh"},
        {"id": 17, "first_name": "Lakshmi"},
        {"id": 18, "first_name": "Rajesh"},
        {"id": 19, "first_name": "Fatima"},
        {"id": 20, "first_name": "Chen Wei"},     # non-Indian name
    ]

    # ── FOR REAL DATA: uncomment this to read from CSV ──
    # sample_names = []
    # with open(INPUT_CSV, "r") as f:
    #     reader = csv.DictReader(f)
    #     for row in reader:
    #         sample_names.append({"id": row["id"], "first_name": row["first_name"]})

    names = sample_names
    total = len(names)
    all_results = []

    print(f"Processing {total} names in batches of {BATCH_SIZE}...")

    for i in range(0, total, BATCH_SIZE):
        batch = names[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} names)...", end=" ", flush=True)

        try:
            results = detect_gender_batch(batch)
            all_results.extend(results)
            print("done")
        except Exception as e:
            print(f"ERROR: {e}")
            # Mark failed batch as unknown
            for n in batch:
                all_results.append({"id": n["id"], "gender": "unknown", "confidence": "low"})

        # Rate limit safety — 1s pause between batches
        if i + BATCH_SIZE < total:
            time.sleep(1)

    # ── WRITE OUTPUT ──
    # Build lookup from original names
    name_lookup = {str(n["id"]): n["first_name"] for n in names}

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "first_name", "gender", "confidence"])
        writer.writeheader()
        for r in all_results:
            writer.writerow({
                "id": r["id"],
                "first_name": name_lookup.get(str(r["id"]), ""),
                "gender": r["gender"],
                "confidence": r.get("confidence", "medium"),
            })

    print(f"\n✅ Results written to {OUTPUT_CSV}")

    # ── QUICK SUMMARY ──
    from collections import Counter
    gender_counts = Counter(r["gender"] for r in all_results)
    print(f"\nSummary: {dict(gender_counts)}")
    print(f"Cost estimate for 20K names: ~$0.50-1.00 (using Sonnet)")


if __name__ == "__main__":
    main()
