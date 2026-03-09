"""
Orb ACH Event Ingestion Script (with Backfill support)
-------------------------------------------------------
Reads a cleaned ACH CSV and sends historical events to Orb using the
Backfill API, which is required for events older than the grace period.

The three-step backfill process:
  1. Create a backfill for the timeframe (returns a backfill_id)
  2. Ingest events referencing the backfill_id
  3. Close the backfill so changes are reflected in Orb

Usage:
  python3 ingest_to_orb.py <cleaned_csv> <orb_api_key>

Example:
  python3 ingest_to_orb.py output.csv orb_test_abc123
"""

import csv
import json
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone


ORB_BASE_URL = "https://api.withorb.com/v1"
BATCH_SIZE = 100


def month_to_timeframe(month_str):
    """
    Convert "02-2026" to start and end ISO 8601 timestamps for the month.
    e.g. timeframe_start = "2026-02-01T00:00:00Z"
         timeframe_end   = "2026-03-01T00:00:00Z"  (exclusive)
    """
    dt = datetime.strptime(month_str, "%m-%Y")
    start = dt.replace(day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    if dt.month == 12:
        end = dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    else:
        end = dt.replace(month=dt.month + 1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def month_to_timestamp(month_str):
    """Convert "02-2026" to "2026-02-01T00:00:00Z" for individual events."""
    dt = datetime.strptime(month_str, "%m-%Y")
    return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def orb_request(method, path, api_key, body=None):
    """Make an authenticated request to the Orb API."""
    url = f"{ORB_BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method=method
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def create_backfill(month_str, api_key):
    """Step 1: Create a backfill for the given month's timeframe."""
    timeframe_start, timeframe_end = month_to_timeframe(month_str)
    print(f"Creating backfill for timeframe {timeframe_start} -> {timeframe_end}...")
    response = orb_request("POST", "/events/backfills", api_key, {
        "timeframe_start": timeframe_start,
        "timeframe_end": timeframe_end,
        "replace_existing_events": False,
    })
    backfill_id = response["id"]
    print(f"Backfill created: {backfill_id} (status: {response['status']})\n")
    return backfill_id


def build_events(row):
    """Build up to two Orb events from a single CSV row. Skips zero counts."""
    events = []
    timestamp = month_to_timestamp(row["month"])

    for transfer_type in ("standard", "sameday"):
        count = int(row[transfer_type])
        if count == 0:
            continue
        events.append({
            "idempotency_key": f"{row['transaction_id']}-{transfer_type}",
            "external_customer_id": row["account_id"],
            "event_name": "ach_transfer",
            "timestamp": timestamp,
            "properties": {
                "transfer_type": transfer_type,
                "count": count,
                "bank_id": row["bank_id"],
                "account_type": row["account_type"],
            }
        })
    return events


def send_batch(events, backfill_id, api_key):
    """Step 2: Ingest a batch of events, referencing the backfill_id."""
    path = f"/ingest?debug=true&backfill_id={backfill_id}"
    return orb_request("POST", path, api_key, {"events": events})


def close_backfill(backfill_id, api_key):
    """Step 3: Close the backfill so changes are reflected in Orb."""
    print(f"\nClosing backfill {backfill_id}...")
    response = orb_request("POST", f"/events/backfills/{backfill_id}/close", api_key)
    print(f"Backfill closed (status: {response['status']})")
    return response


def ingest(csv_path, api_key):
    # Read and build all events
    all_events = []
    skipped = 0
    month_str = None

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if month_str is None:
                month_str = row["month"]
            events = build_events(row)
            if not events:
                skipped += 1
            all_events.extend(events)

    print(f"Built {len(all_events)} events ({skipped} rows skipped - all-zero counts)\n")

    # Step 1: Create backfill
    backfill_id = create_backfill(month_str, api_key)

    # Step 2: Ingest events in batches
    print(f"Ingesting events in batches of {BATCH_SIZE}...")
    total_ingested = []
    total_duplicates = []
    total_failed = []

    for i in range(0, len(all_events), BATCH_SIZE):
        batch = all_events[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        print(f"  Batch {batch_num}: sending {len(batch)} events...", end=" ")

        try:
            response = send_batch(batch, backfill_id, api_key)
            ingested = response.get("debug", {}).get("ingested", [])
            duplicates = response.get("debug", {}).get("duplicate", [])
            failed = response.get("validation_failed", [])

            total_ingested.extend(ingested)
            total_duplicates.extend(duplicates)
            total_failed.extend(failed)

            print(f"v {len(ingested)} ingested, {len(duplicates)} duplicate, {len(failed)} failed")

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8")
            print(f"\nHTTP {e.code}: {body}")
            print(f"\nBackfill {backfill_id} was NOT closed due to error. You can revert it in the Orb dashboard.")
            sys.exit(1)

    # Step 3: Close the backfill
    if total_failed:
        print(f"\n{len(total_failed)} events failed validation - reviewing before closing backfill:")
        for f in total_failed:
            print(f"  - {f['idempotency_key']}: {f['validation_errors']}")
        answer = input("\nClose backfill anyway? (yes/no): ").strip().lower()
        if answer != "yes":
            print("Backfill left open. You can close or revert it manually in the Orb dashboard.")
            sys.exit(0)

    close_backfill(backfill_id, api_key)

    # Summary
    print(f"\n{'='*50}")
    print(f"Total ingested:    {len(total_ingested)}")
    print(f"Total duplicates:  {len(total_duplicates)}")
    print(f"Total failed:      {len(total_failed)}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 ingest_to_orb.py <cleaned_csv> <orb_api_key>")
        sys.exit(1)

    ingest(sys.argv[1], sys.argv[2])