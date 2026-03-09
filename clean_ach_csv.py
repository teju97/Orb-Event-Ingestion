"""
ACH Transaction CSV Cleaner
----------------------------
Cleans raw ACH usage data before ingestion into billing platform.

Transformations applied:
  1. Coerce blank standard/sameday to 0
  2. Strip commas from numbers (e.g. "1,290" -> 1290)
  3. Flag scientific notation transaction IDs (e.g. 8.90E+08) as CORRUPTED-XXX
  4. Normalize plain integer IDs to strings (e.g. 92427375 -> "92427375")
"""

import csv
import re
import sys


def is_scientific_notation(value: str) -> bool:
    """Detect values like 8.90E+08 that Excel mangled from hex strings."""
    return bool(re.match(r"^\d+\.\d+[eE][+\-]\d+$", value.strip()))


def is_plain_integer(value: str) -> bool:
    """Detect all-digit strings that were originally hex IDs."""
    return bool(re.match(r"^\d+$", value.strip()))


def is_hex_id(value: str) -> bool:
    """Detect normal 8-character hex transaction IDs like 126dff1e."""
    return bool(re.match(r"^[0-9a-fA-F]{8}$", value.strip()))


def clean_number(value: str) -> int:
    """Strip commas and coerce blanks to 0."""
    if value.strip() == "":
        return 0
    return int(value.replace(",", ""))


def clean_transaction_id(value: str, corrupted_counter: list) -> tuple[str, str | None]:
    """
    Normalize transaction ID. Returns (cleaned_id, warning_message).
    corrupted_counter is a mutable list used to track the corruption index.
    """
    v = value.strip()

    if is_hex_id(v):
        # Normal hex ID — leave as-is
        return v, None

    elif is_plain_integer(v):
        # All-digit hex that Excel stored as a number — valid, just ensure string
        return v, None

    elif is_scientific_notation(v):
        # Excel mangled a hex ID with letters into scientific notation — unrecoverable
        corrupted_counter[0] += 1
        synthetic_id = f"CORRUPTED-{corrupted_counter[0]:03d}"
        warning = f"Unrecoverable transaction ID '{v}' replaced with '{synthetic_id}'"
        return synthetic_id, warning

    else:
        # Unknown format — pass through with a warning
        return v, f"Unrecognized transaction ID format: '{v}'"


def clean_csv(input_path: str, output_path: str):
    corrupted_counter = [0]  # mutable so clean_transaction_id can increment it
    warnings = []
    rows_processed = 0

    with open(input_path, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)

        output_rows = []
        for i, row in enumerate(reader, start=2):  # start=2 because row 1 is header
            cleaned_id, warning = clean_transaction_id(
                row["transaction_id"], corrupted_counter
            )
            if warning:
                warnings.append(f"  Row {i}: {warning}")

            output_rows.append({
                "account_id":     row["account_id"].strip(),
                "month":          row["month"].strip(),
                "transaction_id": cleaned_id,
                "account_type":   row["account_type"].strip(),
                "bank_id":        row["bank_id"].strip(),
                "standard":       clean_number(row["standard"]),
                "sameday":        clean_number(row["sameday"]),
            })
            rows_processed += 1

    with open(output_path, "w", newline="", encoding="utf-8") as outfile:
        fieldnames = ["account_id", "month", "transaction_id", "account_type",
                      "bank_id", "standard", "sameday"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    # Print summary
    print(f"✓ Cleaned {rows_processed} rows")
    print(f"✓ Output written to: {output_path}")
    if warnings:
        print(f"\n⚠ {len(warnings)} warning(s):")
        for w in warnings:
            print(w)
    else:
        print("✓ No warnings")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python clean_ach_csv.py <input_csv> <output_csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    clean_csv(input_path, output_path)