"""
Generates deliberately messy source data so the pipeline has something to clean.

Everything here is synthetic and produced with a fixed random seed, so running
this script twice gives you the exact same "before" data every time.

Output:
  data/raw/expenses_raw.csv         - a CSV export full of real-world mess
  data/raw/scanned_report_01.txt    - three plain-text "scanned" expense reports
  data/raw/scanned_report_02.txt      with irregular, inconsistent layouts
  data/raw/scanned_report_03.txt
"""

import csv
import os
import random

random.seed(42)

RAW_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")

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

# Every one of these is a real way someone could have typed or exported
# a category name. The pipeline has to figure out what they mean.
CATEGORY_VARIANTS = {
    "Travel": ["Travel", "TRAVEL", "travel", "Travle", "Trvel", " Travel "],
    "Office Supplies": [
        "Office Supplies",
        "office supplies",
        "Offce Supplies",
        "Office_Supplies",
        "OFFICE SUPPLIES",
    ],
    "Software": ["Software", "SOFTWARE", "Softwar", "softwear", " Software"],
    "Meals & Entertainment": [
        "Meals & Entertainment",
        "Meals and Entertainment",
        "meals & ent",
        "Meals&Entertainment",
        "MEALS & ENTERTAINMENT",
    ],
    "Utilities": ["Utilities", "Utilties", "utilities", "UTILITIES", "Utillities"],
    "Marketing": ["Marketing", "Marketting", "marketing", "MARKETING", "Markeing"],
    "Professional Services": [
        "Professional Services",
        "Proffesional Services",
        "professional services",
        "Prof. Services",
        "PROFESSIONAL SERVICES",
    ],
    "Equipment": ["Equipment", "Equipmnet", "equipment", "EQUIPMENT", "Equpiment"],
}

FIRST_NAMES = ["Maria", "James", "Wei", "Fatima", "Diego", "Priya", "Sam", "Olu", "Anna", "Liam"]
LAST_NAMES = ["Rivera", "Chen", "Okafor", "Singh", "Novak", "Rossi", "Kim", "Dubois", "Schmidt", "Patel"]
DEPARTMENTS = ["Marketing", "Engineering", "Sales", "Operations", "Finance", "Support"]

DESCRIPTIONS = {
    "Travel": ["Flight to client site", "Rental car", "Train ticket", "Hotel - conference", "Taxi to airport"],
    "Office Supplies": ["Printer paper", "Desk chair", "Notebooks and pens", "Toner cartridges", "Whiteboard markers"],
    "Software": ["Annual SaaS license", "API usage overage", "Design tool subscription", "Dev tools seat", "Cloud storage plan"],
    "Meals & Entertainment": ["Client dinner", "Team lunch", "Coffee with prospect", "Conference catering", "Holiday party"],
    "Utilities": ["Internet - satellite office", "Phone plan reimbursement", "Electricity - co-working space", "Water bill - office"],
    "Marketing": ["Trade show booth", "Sponsored social post", "Print flyers", "Email platform fee", "Swag order"],
    "Professional Services": ["Legal consultation", "Accounting fees", "Freelance designer", "Contract recruiter", "Notary fee"],
    "Equipment": ["Laptop stand", "Monitor", "Webcam", "Warehouse shelving", "Label printer"],
}

# A handful of different date formats, all meaning valid calendar dates.
def random_date_2024():
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return month, day, 2024

def format_date_messy(month, day, year):
    style = random.choice(["mdy_slash", "iso", "dmy_dash_mon", "long", "mdy_dash", "two_digit_year"])
    months_abbrev = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    months_full = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    if style == "mdy_slash":
        return f"{month}/{day}/{year}"
    if style == "iso":
        return f"{year}-{month:02d}-{day:02d}"
    if style == "dmy_dash_mon":
        return f"{day:02d}-{months_abbrev[month - 1]}-{year}"
    if style == "long":
        return f"{months_full[month - 1]} {day}, {year}"
    if style == "mdy_dash":
        return f"{month:02d}-{day:02d}-{year}"
    if style == "two_digit_year":
        return f"{month}/{day}/{str(year)[2:]}"
    return f"{month}/{day}/{year}"


def make_row(row_id):
    canonical = random.choice(CANONICAL_CATEGORIES)
    variant = random.choice(CATEGORY_VARIANTS[canonical])
    month, day, year = random_date_2024()
    amount = round(random.uniform(8, 2400), 2)
    submitted_by = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    return {
        "row_id": row_id,
        "date": format_date_messy(month, day, year),
        "category": variant,
        "amount": amount,
        "description": random.choice(DESCRIPTIONS[canonical]),
        "submitted_by": submitted_by,
        "department": random.choice(DEPARTMENTS),
    }


def build_csv_rows(n=520):
    rows = []
    for i in range(1, n + 1):
        rows.append(make_row(i))

    # Sprinkle in real-world mess on top of the base rows. Each corruption
    # pass gets its own disjoint slice of row indices, sampled once up front,
    # so no row is corrupted twice and the counts below map directly to the
    # rejection reasons the pipeline produces.
    counts = {
        "duplicate": 22,
        "missing_amount": 12,
        "missing_date": 10,
        "garbled_date": 8,
        "negative_amount": 9,
        "blank_category": 7,
    }
    picks = random.sample(range(n), sum(counts.values()))
    cursor = 0
    slices = {}
    for key, count in counts.items():
        slices[key] = picks[cursor:cursor + count]
        cursor += count

    # 1. Exact duplicates (someone re-uploaded the same export). Copy before
    # any corruption pass runs, so the pair stays identical.
    for i in slices["duplicate"]:
        rows.append(dict(rows[i]))

    # 2. Missing amount.
    for i in slices["missing_amount"]:
        rows[i]["amount"] = ""

    # 3. Missing date.
    for i in slices["missing_date"]:
        rows[i]["date"] = ""

    # 4. Garbled / unparseable date.
    for i in slices["garbled_date"]:
        rows[i]["date"] = random.choice(["N/A", "see attached", "13/45/2024", "unknown", "--"])

    # 5. Zero or negative amount (refunds entered wrong, typos).
    for i in slices["negative_amount"]:
        rows[i]["amount"] = round(random.uniform(-200, 0), 2)

    # 6. Blank category.
    for i in slices["blank_category"]:
        rows[i]["category"] = ""

    random.shuffle(rows)
    for i, row in enumerate(rows, start=1):
        row["row_id"] = i
    return rows


def write_csv(rows):
    os.makedirs(RAW_DIR, exist_ok=True)
    path = os.path.join(RAW_DIR, "expenses_raw.csv")
    fieldnames = ["row_id", "date", "category", "amount", "description", "submitted_by", "department"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {path}")


# --- "Scanned report" text files -------------------------------------------
# These simulate someone OCR'ing or manually typing up paper expense reports.
# Every file uses a slightly different layout on purpose.

def scanned_report_01():
    return """EXPENSE REPORT - REGION: NORTHEAST
Employee: J. Rivera          Dept: Marketing
Period: March 2024
------------------------------------------------

1) Date: 03/14/2024   Category: Marketing      Amount: $1,240.00
   Desc: Trade show booth deposit

2) Date 03/15/2024  Categry: Travle   Amt: $455.10
   Description: Flight to Chicago for client visit

3) Date: 03/18/2024   Category: Meals & Entertainment   Amount $88.40
   Desc: Client dinner - downtown

4)  Date: 03/2024 (day unclear)   Category: Software   Amount: $19.99
    Desc: Design tool subscription

5) Date: 03/22/2024  Category: Office Supplies  Amount: $-15.00
   Desc: Return credit, notebooks

Submitted by J. Rivera, approved by manager on file.
"""


def scanned_report_02():
    return """       MONTHLY EXPENSE LOG
   Submitted by: A. Chen (Engineering)

  Mar 2  | Equipment      | $340.00  | Monitor for home office
  Mar 5  | Utilities      | $61.25   | Internet - satellite office
  Mar 9  | Softwar        | $12.00   | Cloud storage plan
  Mar 11 | Travel         | $         | Rental car (receipt attached, amount TBD)
  Mar 19 | Professional Services | $600.00 | Freelance designer
  Mar 25 | equipment      | $75.50   | Webcam

  -- end of log, page 1 of 1 --
"""


def scanned_report_03():
    return """EXP. REPORT  //  Dept: Sales  //  Employee: D. Novak

>> 04-01-2024 :: MARKETING :: 210.00 :: Print flyers for regional event
>> 04-03-2024 :: Meals and Entertainment :: 42.75 :: Team lunch
>> unknown date :: Travel :: 890.00 :: Hotel - conference (dates on receipt only)
>> 04-10-2024 :: UTILITIES :: 0.00 :: Phone plan reimbursement (denied, $0)
>> 04-12-2024 :: Proffesional Services :: 150.00 :: Notary fee
>> 04-14-2024 :: Office_Supplies :: 33.10 :: Toner cartridges

note: scanner skipped line 7, illegible
"""


def write_scanned_reports():
    os.makedirs(RAW_DIR, exist_ok=True)
    reports = {
        "scanned_report_01.txt": scanned_report_01(),
        "scanned_report_02.txt": scanned_report_02(),
        "scanned_report_03.txt": scanned_report_03(),
    }
    for name, content in reports.items():
        path = os.path.join(RAW_DIR, name)
        with open(path, "w") as f:
            f.write(content)
        print(f"wrote {path}")


if __name__ == "__main__":
    rows = build_csv_rows()
    write_csv(rows)
    write_scanned_reports()
    print("done. messy source data is in data/raw/")
