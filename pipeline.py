"""
Staged ETL pipeline: extract -> clean -> validate -> load.

Reads the messy files in data/raw/, normalizes dates and categories, drops
duplicates, rejects rows that don't meet the validation rules (logging why
for every single one), and writes what's left into a SQLite warehouse.

Run it with:
    python pipeline.py
"""

import csv
import difflib
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(__file__)
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
LOG_DIR = os.path.join(BASE_DIR, "logs")
DB_PATH = os.path.join(BASE_DIR, "warehouse.db")

CANONICAL_CATEGORIES = [
    "Travel",
    "Office Supplies",
    "Software",
    "Meals & Entertainment",
    "Utilities",
    "Marketing",
    "Professional Services",
    "Equipment",
]
_CATEGORY_LOOKUP = {c.lower(): c for c in CANONICAL_CATEGORIES}
_CATEGORY_NORMALIZED = {re.sub(r"[^a-z]", "", c.lower()): c for c in CANONICAL_CATEGORIES}

DATE_FORMATS = [
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%d-%b-%Y",
    "%B %d, %Y",
    "%m-%d-%Y",
    "%m/%d/%y",
    "%b %d %Y",
    "%b %d, %Y",
]

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "pipeline.log"), mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("pipeline")


# ---------------------------------------------------------------- extract --

def extract_csv(path):
    """Read the raw CSV export as-is, no cleaning yet."""
    records = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            records.append({
                "source": "csv_export",
                "source_ref": f"row_id={row.get('row_id')}",
                "raw_date": row.get("date", ""),
                "raw_category": row.get("category", ""),
                "raw_amount": row.get("amount", ""),
                "description": row.get("description", ""),
                "submitted_by": row.get("submitted_by") or None,
                "department": row.get("department") or None,
            })
    log.info("extract: read %d rows from %s", len(records), os.path.basename(path))
    return records


# Three line shapes show up across the "scanned" text reports. Each report
# uses one shape consistently, but a real OCR pipeline has to be ready for
# more than one, so we try all three against every file.
NUMBERED_BLOCK = re.compile(
    r"Date:?\s*(?P<date>[^\n]*?)\s+Categ\w*:?\s*(?P<category>[^\n]*?)\s+A(?:mt|mount):?\s*\$?(?P<amount>[^\n]*?)\n\s*Desc\w*:?\s*(?P<description>[^\n]*)",
    re.MULTILINE,
)
PIPE_TABLE = re.compile(
    r"^\s*(?P<date>[A-Za-z]{3}\s+\d{1,2})\s*\|\s*(?P<category>[^|]+?)\s*\|\s*\$?(?P<amount>[^|]*?)\s*\|\s*(?P<description>.+?)\s*$",
    re.MULTILINE,
)
COLON_DELIM = re.compile(
    r"^>>\s*(?P<date>[^:]+?)\s*::\s*(?P<category>[^:]+?)\s*::\s*(?P<amount>[^:]+?)\s*::\s*(?P<description>.+?)\s*$",
    re.MULTILINE,
)


def extract_scanned_reports(raw_dir):
    """Parse the plain-text 'scanned' reports. Each file gets tried against
    every known layout pattern; whichever one matches is what that file uses.
    """
    records = []
    for fname in sorted(os.listdir(raw_dir)):
        if not fname.endswith(".txt"):
            continue
        path = os.path.join(raw_dir, fname)
        with open(path) as f:
            text = f.read()

        found = 0
        for i, m in enumerate(NUMBERED_BLOCK.finditer(text), start=1):
            records.append({
                "source": fname,
                "source_ref": f"entry_{i}",
                "raw_date": m.group("date").strip(),
                "raw_category": m.group("category").strip(),
                "raw_amount": m.group("amount").strip(),
                "description": m.group("description").strip(),
                "submitted_by": None,
                "department": None,
            })
            found += 1

        for i, m in enumerate(PIPE_TABLE.finditer(text), start=1):
            # These rows only ever show a month + day, so we assume the same
            # reporting year as the rest of the batch (2024).
            records.append({
                "source": fname,
                "source_ref": f"row_{i}",
                "raw_date": f"{m.group('date').strip()} 2024",
                "raw_category": m.group("category").strip(),
                "raw_amount": m.group("amount").strip(),
                "description": m.group("description").strip(),
                "submitted_by": None,
                "department": None,
            })
            found += 1

        for i, m in enumerate(COLON_DELIM.finditer(text), start=1):
            records.append({
                "source": fname,
                "source_ref": f"line_{i}",
                "raw_date": m.group("date").strip(),
                "raw_category": m.group("category").strip(),
                "raw_amount": m.group("amount").strip(),
                "description": m.group("description").strip(),
                "submitted_by": None,
                "department": None,
            })
            found += 1

        log.info("extract: parsed %d entries from %s", found, fname)
    return records


def extract():
    csv_path = os.path.join(RAW_DIR, "expenses_raw.csv")
    records = extract_csv(csv_path)
    records += extract_scanned_reports(RAW_DIR)
    log.info("extract: %d raw records total", len(records))
    return records


# ------------------------------------------------------------ clean utils --

def clean_date(raw_date):
    if not raw_date or not raw_date.strip():
        return None, "missing_date"
    text = raw_date.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat(), None
        except ValueError:
            continue
    return None, "unparseable_date"


def clean_category(raw_category):
    if not raw_category or not raw_category.strip():
        return None, "missing_category"
    text = raw_category.strip().replace("_", " ")
    lowered = text.lower()
    if lowered in _CATEGORY_LOOKUP:
        return _CATEGORY_LOOKUP[lowered], None

    normalized = re.sub(r"[^a-z&]", "", lowered)
    if normalized in _CATEGORY_NORMALIZED:
        return _CATEGORY_NORMALIZED[normalized], None

    match = difflib.get_close_matches(lowered, _CATEGORY_LOOKUP.keys(), n=1, cutoff=0.6)
    if match:
        return _CATEGORY_LOOKUP[match[0]], None
    return None, "unrecognized_category"


def clean_amount(raw_amount):
    if raw_amount is None or not str(raw_amount).strip():
        return None, "missing_amount"
    text = str(raw_amount).strip().replace("$", "").replace(",", "")
    try:
        value = round(float(text), 2)
    except ValueError:
        return None, "invalid_amount"
    if value <= 0:
        return None, "non_positive_amount"
    return value, None


# --------------------------------------------------------- clean/validate --

def clean_and_validate(raw_records):
    clean_rows = []
    rejected_rows = []
    seen = set()

    for rec in raw_records:
        date_val, err = clean_date(rec["raw_date"])
        if err:
            rejected_rows.append({**rec, "reason": err})
            continue

        category_val, err = clean_category(rec["raw_category"])
        if err:
            rejected_rows.append({**rec, "reason": err})
            continue

        amount_val, err = clean_amount(rec["raw_amount"])
        if err:
            rejected_rows.append({**rec, "reason": err})
            continue

        description = (rec.get("description") or "").strip()
        dedup_key = (date_val, category_val, amount_val, description.lower())
        if dedup_key in seen:
            rejected_rows.append({**rec, "reason": "duplicate_row"})
            continue
        seen.add(dedup_key)

        clean_rows.append({
            "source": rec["source"],
            "source_ref": rec["source_ref"],
            "date": date_val,
            "category": category_val,
            "amount": amount_val,
            "description": description,
            "submitted_by": rec.get("submitted_by"),
            "department": rec.get("department"),
        })

    for row in rejected_rows:
        log.warning(
            "rejected [%s] source=%s ref=%s raw_date=%r raw_category=%r raw_amount=%r",
            row["reason"], row["source"], row["source_ref"],
            row["raw_date"], row["raw_category"], row["raw_amount"],
        )

    log.info(
        "clean/validate: %d rows accepted, %d rows rejected",
        len(clean_rows), len(rejected_rows),
    )
    return clean_rows, rejected_rows


# ------------------------------------------------------------------- load --

SCHEMA = """
CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_ref TEXT,
    date TEXT NOT NULL,
    category TEXT NOT NULL,
    amount REAL NOT NULL,
    description TEXT,
    submitted_by TEXT,
    department TEXT,
    loaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rejected_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_ref TEXT,
    reason TEXT NOT NULL,
    raw_date TEXT,
    raw_category TEXT,
    raw_amount TEXT,
    description TEXT,
    rejected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    rows_extracted INTEGER NOT NULL,
    rows_loaded INTEGER NOT NULL,
    rows_rejected INTEGER NOT NULL
);
"""


def load(clean_rows, rejected_rows, raw_count):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)

    now = datetime.now(timezone.utc).isoformat()

    conn.executemany(
        """INSERT INTO expenses
           (source, source_ref, date, category, amount, description, submitted_by, department, loaded_at)
           VALUES (:source, :source_ref, :date, :category, :amount, :description, :submitted_by, :department, :loaded_at)""",
        [{**row, "loaded_at": now} for row in clean_rows],
    )

    conn.executemany(
        """INSERT INTO rejected_rows
           (source, source_ref, reason, raw_date, raw_category, raw_amount, description, rejected_at)
           VALUES (:source, :source_ref, :reason, :raw_date, :raw_category, :raw_amount, :description, :rejected_at)""",
        [{**row, "rejected_at": now} for row in rejected_rows],
    )

    conn.execute(
        "INSERT INTO pipeline_runs (run_at, rows_extracted, rows_loaded, rows_rejected) VALUES (?, ?, ?, ?)",
        (now, raw_count, len(clean_rows), len(rejected_rows)),
    )

    conn.commit()
    conn.close()
    log.info("load: wrote %d clean rows and %d rejected rows to %s", len(clean_rows), len(rejected_rows), DB_PATH)


def main():
    raw_records = extract()
    clean_rows, rejected_rows = clean_and_validate(raw_records)
    load(clean_rows, rejected_rows, len(raw_records))
    log.info("pipeline complete: %s", DB_PATH)


if __name__ == "__main__":
    main()
