# ACH Event Ingestion Pipeline

Two scripts for cleaning raw ACH transaction data and ingesting it into [Orb](https://www.withorb.com) as billable events.

## Overview

Raw ACH usage data exported from Excel often contains formatting issues that need to be cleaned before ingestion. This pipeline handles that cleaning and then sends the data to Orb using the Backfill API, which is required for historical events older than Orb's ingestion grace period.

```
input CSV → clean_ach_csv.py → cleaned CSV → ingest_to_orb.py → Orb
```

## Requirements

- Python 3.10+
- An Orb API key (generate one in the Orb dashboard under Organization Settings)

## Scripts

### 1. `clean_ach_csv.py`

Cleans a raw ACH CSV before ingestion.

**Transformations applied:**
- Coerces blank `standard`/`sameday` values to `0` (blanks represent zero transactions, not missing data)
- Strips commas from numbers (e.g. `"1,290"` → `1290`)
- Detects transaction IDs mangled into scientific notation by Excel (e.g. `8.90E+08`) and replaces them with a synthetic ID (e.g. `CORRUPTED-001`). The usage values are preserved and are still billable; only the ID is unrecoverable.
- Normalizes all-digit transaction IDs (e.g. `92427375`) to strings, preserving their original value

**Usage:**
```bash
python3 clean_ach_csv.py <input_csv> <output_csv>
```

**Example:**
```bash
python3 clean_ach_csv.py raw_data.csv cleaned_data.csv
```

**Example output:**
```
✓ Cleaned 78 rows
✓ Output written to: cleaned_data.csv

⚠ 1 warning(s):
  Row 3: Unrecoverable transaction ID '8.90E+08' replaced with 'CORRUPTED-001'
```

---

### 2. `ingest_to_orb.py`

Ingests a cleaned ACH CSV into Orb using the [Backfill API](https://docs.withorb.com/api-reference/event/create-backfill).

Each CSV row produces up to two events — one for `standard` ACH transfers and one for `sameday` — since these are separate billable dimensions. Rows where both counts are zero are skipped.

**The three-step backfill process:**
1. **Create** a backfill for the data's timeframe
2. **Ingest** events referencing the backfill ID
3. **Close** the backfill so changes are reflected in Orb

Events are sent in batches of 100. If any events fail validation, the script will pause and ask for confirmation before closing the backfill.

**Usage:**
```bash
python3 ingest_to_orb.py <cleaned_csv> <orb_api_key>
```

**Example:**
```bash
python3 ingest_to_orb.py cleaned_data.csv orb_test_abc123
```

**Example output:**
```
Built 81 events (18 rows skipped - all-zero counts)

Creating backfill for timeframe 2026-02-01T00:00:00Z -> 2026-03-01T00:00:00Z...
Backfill created: Gbc8cSquC5Ye36Pv (status: pending)

Ingesting events in batches of 100...
  Batch 1: sending 81 events... 81 ingested, 0 duplicate, 0 failed

Closing backfill Gbc8cSquC5Ye36Pv...
Backfill closed (status: pending)

==================================================
Total ingested:    81
Total duplicates:  0
Total failed:      0
```

**Note:** After closing, the backfill status will show as `pending` briefly before moving to `reflected`. This is normal — Orb processes backfills asynchronously. You can check the status with:

```bash
curl https://api.withorb.com/v1/events/backfills/<backfill_id> \
  -H "Authorization: Bearer YOUR_API_KEY"
```

## Event Schema

Each event ingested into Orb has the following shape:

```json
{
  "idempotency_key": "126dff1e-standard",
  "external_customer_id": "acme",
  "event_name": "ach_transfer",
  "timestamp": "2026-02-01T00:00:00Z",
  "properties": {
    "transfer_type": "standard",
    "count": 293,
    "bank_id": "bank_green",
    "account_type": "checking"
  }
}
```

## Running the Full Pipeline

```bash
python3 clean_ach_csv.py raw_data.csv cleaned_data.csv
python3 ingest_to_orb.py cleaned_data.csv YOUR_ORB_API_KEY
```

## Security Note

Never commit your Orb API key to version control. Always pass it as a command line argument or via an environment variable.
