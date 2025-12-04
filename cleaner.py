"""
Portable Data Cleaner (MVP)
---------------------------

This script:

1. Reads one or more lead files from the /input folder (CSV, XLSX, ODS).
2. Normalises column names and cleans text/postcodes/phone numbers.
3. Loads any DNC lists from /dnc_lists and prepares a set of blocked numbers.
4. Deduplicates leads using:
      - primary phone number (first priority), OR
      - address + postcode (second priority).
   When duplicates are found, it MERGES their information into one "master" lead.
5. Splits rows into:
      - cleaned leads
      - duplicate/archive rows
      - invalid (no phone)
      - DNC hits
6. Outputs cleaned data as ODS/XLSX (configurable), plus CSVs for the other sets.
7. Generates a summary report + simple log file for each run.

Everything is designed to be:

- Portable (run from a USB with WinPython)
- Transparent (no data silently thrown away)
- Auditable (reports + logs + duplicate/archive files)
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd  # main data-handling library


# === PATH SETUP ==================================================================

# Base directory = where cleaner.py lives (USB project root)
BASE_DIR = Path(__file__).resolve().parent

# Core folders inside the project
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
DNC_DIR = BASE_DIR / "dnc_lists"

# Output subfolders
CLEANED_DIR = OUTPUT_DIR / "cleaned"
DUP_DIR = OUTPUT_DIR / "duplicates"
INVALID_DIR = OUTPUT_DIR / "invalid"
DNC_HITS_DIR = OUTPUT_DIR / "dnc_hits"
REPORTS_DIR = OUTPUT_DIR / "reports"

# The set of phone-related columns we work with.
# These columns will be created if they don't exist.
PHONE_COLUMNS = ["Mobile", "Landline", "AltNumber"]


# === CONFIG LOADING ===============================================================

def load_config() -> dict:
    """
    Load settings from config.json.
    If config.json is missing, fall back to sensible defaults.
    """
    config_path = BASE_DIR / "config.json"
    if not config_path.exists():
        print("⚠ config.json not found, using default settings.")
        return {
            "output_format": "ods",                 # 'ods', 'xlsx', or 'both'
            "duplicate_by_phone": True,            # use phone-based dedupe
            "duplicate_by_name_postcode": False,   # (not used in MVP)
            "duplicate_by_address_postcode": True, # use address+postcode dedupe
            "normalise_names": True,
            "normalise_postcodes": True,
            "normalise_addresses": True,
            "no_phone_behavior": "invalid",        # what to do if no phone
            "merge_notes": True,                   # merge Notes fields
        }

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


# === FOLDER ENSURE ================================================================

def ensure_directories() -> None:
    """
    Make sure all the required folders exist.
    If they don't, create them.
    """
    for path in [
        INPUT_DIR,
        CLEANED_DIR,
        DUP_DIR,
        INVALID_DIR,
        DNC_HITS_DIR,
        REPORTS_DIR,
        LOG_DIR,
        DNC_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


# === INPUT HANDLING ===============================================================

def list_input_files():
    """
    Return a list of all supported files in /input.
    Supported extensions: .csv, .xlsx, .ods
    """
    files = []
    for ext in (".csv", ".xlsx", ".ods"):
        files.extend(INPUT_DIR.glob(f"*{ext}"))
    return files


def read_input_file(path: Path) -> pd.DataFrame:
    """
    Read a single input file into a DataFrame.

    - CSV  -> pd.read_csv
    - XLSX -> pd.read_excel (openpyxl engine)
    - ODS  -> pd.read_excel (odf engine)

    Adds a special column __source_file to remember where each row came from.
    """
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix == ".xlsx":
        df = pd.read_excel(path, engine="openpyxl")
    elif suffix == ".ods":
        df = pd.read_excel(path, engine="odf")
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    # Keep track of which original file each row came from
    df["__source_file"] = path.name
    return df


# === HEADER NORMALISATION =========================================================

# Mapping of messy header names to our standard header names.
# (all keys are "lowercased" versions of potential input headers)
HEADER_MAP = {
    # Names
    "firstname": "FirstName",
    "first_name": "FirstName",
    "fname": "FirstName",
    "lastname": "LastName",
    "last_name": "LastName",
    "surname": "LastName",
    "name": "Name",

    # Addresses
    "address": "Address1",
    "address1": "Address1",
    "address 1": "Address1",
    "address line 1": "Address1",
    "address2": "Address2",
    "address 2": "Address2",
    "address line 2": "Address2",
    "town": "Town",
    "city": "Town",

    # Postcodes
    "postcode": "Postcode",
    "post code": "Postcode",
    "zip": "Postcode",

    # Phones
    "phone": "Landline",
    "telephone": "Landline",
    "tel": "Landline",
    "landline": "Landline",
    "mobile": "Mobile",
    "mobile phone": "Mobile",
    "mob": "Mobile",
    "alt": "AltNumber",
    "alt phone": "AltNumber",
    "phone number": "AltNumber",
    "phone no": "AltNumber",
    "contact number": "AltNumber",
    "contact no": "AltNumber",
    "number": "AltNumber",


    # Email / Notes
    "email": "Email",
    "e-mail": "Email",
    "notes": "Notes",
    "comments": "Notes",
}

# The core set of columns we want to always exist in the DataFrame.
STANDARD_COLUMNS = [
    "FirstName",
    "LastName",
    "Name",
    "Address1",
    "Address2",
    "Town",
    "Postcode",
    "Landline",
    "Mobile",
    "AltNumber",
    "Email",
    "Notes",
]


def normalise_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename messy columns to our standard names using HEADER_MAP.
    Also makes sure all STANDARD_COLUMNS exist (creates empty ones if missing).
    """
    new_cols = {}

    # Loop over existing columns and map them if we know a better name
    for col in df.columns:
        key = str(col).strip().lower()
        new_cols[col] = HEADER_MAP.get(key, col)  # default to original name

    # Rename columns in the DataFrame
    df = df.rename(columns=new_cols)

    # Ensure all standard columns exist (so later code can rely on them)
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df


# === BASIC STRING UTILITIES =======================================================

def tidy_whitespace(value):
    """
    Normalise whitespace in a value:
    - Convert NaN -> empty string
    - Convert non-strings to string
    - Strip leading/trailing spaces
    - Collapse multiple spaces -> single space

    Used everywhere to keep things clean.
    """
    if pd.isna(value):
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    while "  " in value:
        value = value.replace("  ", " ")
    return value


def normalise_name(value):
    """
    Clean a name field and convert it to Title Case.
    e.g. "MRS SMITH" -> "Mrs Smith"
    """
    value = tidy_whitespace(value)
    if not value:
        return ""
    return value.title()


def normalise_postcode(value):
    """
    Clean and standardise a UK-style postcode:
    - Strip whitespace
    - Uppercase
    - Insert a space before the last 3 characters if possible
      e.g. "cm79tg" -> "CM7 9TG"
    """
    value = tidy_whitespace(value).upper()
    if not value:
        return ""
    # remove any spaces
    value = value.replace(" ", "")
    # insert space before last 3 characters (if length makes sense)
    if len(value) > 3:
        value = value[:-3] + " " + value[-3:]
    return value


def clean_phone(raw: str) -> str:
    """
    Clean and validate a UK-ish phone number.

    Now a bit more forgiving for messy marketing lists:
    - Strips non-digits/+.
    - Handles +44 / 0044 prefixes.
    - If we see a 9–10 digit number that DOESN'T start with 0,
      we assume the leading 0 was dropped and prepend it.
    - Then we apply simple UK rules.
    """
    raw = tidy_whitespace(raw)
    if not raw:
        return ""

    # Keep only digits and '+'.
    digits = []
    for ch in raw:
        if ch.isdigit():
            digits.append(ch)
        elif ch == "+":
            digits.append(ch)
    s = "".join(digits)

    # Handle international-style prefixes.
    if s.startswith("0044"):
        # 0044xxxxxxxxxx -> 0xxxxxxxxxx
        s = "0" + s[4:]
    elif s.startswith("+44"):
        # +44xxxxxxxxxx -> 0xxxxxxxxxx
        s = "0" + s[3:]
    elif s.startswith("44") and len(s) in (11, 12):
        # 44xxxxxxxxx or 44xxxxxxxxxx -> 0xxxxxxxxxx
        s = "0" + s[2:]

    # If it's 9–10 digits and doesn't start with 0,
    # assume the leading 0 was chopped off (very common in lists).
    if not s.startswith("0") and len(s) in (9, 10):
        s = "0" + s

    # Strip any remaining leading '+' just in case.
    if s.startswith("+"):
        s = s[1:]

    # If not purely digits now, reject.
    if not s.isdigit():
        return ""

    # Basic UK-style rules AFTER we've normalised the prefix.
    # 11-digit numbers starting with 0 are acceptable as:
    #   - 07... mobiles
    #   - 01/02/03/08 landlines/non-geographic
    if len(s) == 11 and (
        s.startswith("07")
        or s.startswith("01")
        or s.startswith("02")
        or s.startswith("03")
        or s.startswith("08")
    ):
        return s

    # Some landlines can be stored as 10 digits (without a leading 0 originally),
    # but after the "missing 0" fix above, those will mostly come out as 11 anyway.
    # We'll keep a small allowance here for 10-digit 01/02/03/08 just in case.
    if len(s) == 10 and (
        s.startswith("01")
        or s.startswith("02")
        or s.startswith("03")
        or s.startswith("08")
    ):
        return s

    # Otherwise: treat as invalid.
    return ""


def apply_normalisation(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Apply all text/name/postcode/phone normalisation to the DataFrame.
    This is run AFTER headers are normalised.
    """
    # Drop rows that are totally empty (all columns NaN)
    df = df.dropna(how="all").copy()

    # --- Names ---
    if config.get("normalise_names", True):
        if "FirstName" in df.columns:
            df["FirstName"] = df["FirstName"].apply(normalise_name)
        if "LastName" in df.columns:
            df["LastName"] = df["LastName"].apply(normalise_name)
        if "Name" in df.columns:
            df["Name"] = df["Name"].apply(normalise_name)

    # --- Postcodes ---
    if config.get("normalise_postcodes", True) and "Postcode" in df.columns:
        df["Postcode"] = df["Postcode"].apply(normalise_postcode)

    # --- Addresses ---
    if config.get("normalise_addresses", True):
        for col in ["Address1", "Address2", "Town"]:
            if col in df.columns:
                df[col] = df[col].apply(tidy_whitespace)

    # --- Phone numbers ---
    for col in PHONE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_phone)

    # --- Email & Notes whitespace ---
    for col in ["Email", "Notes"]:
        if col in df.columns:
            df[col] = df[col].apply(tidy_whitespace)

    return df


# === DNC HANDLING =================================================================

def load_dnc_numbers() -> set:
    """
    Load all DNC numbers from CSV files in /dnc_lists.

    Assumes:
      - DNC files are CSV
      - First column contains the number (other columns ignored)

    Returns:
      A set of cleaned phone numbers.
    """
    numbers = set()

    for csv_path in DNC_DIR.glob("*.csv"):
        try:
            dnc_df = pd.read_csv(csv_path)
        except Exception as e:
            print(f"⚠ Failed to read DNC list {csv_path.name}: {e}")
            continue

        if dnc_df.empty:
            continue

        first_col = dnc_df.columns[0]
        for raw in dnc_df[first_col].tolist():
            num = clean_phone(raw)
            if num:
                numbers.add(num)

    if numbers:
        print(f"Loaded {len(numbers)} DNC numbers from {DNC_DIR}")
    else:
        print("No DNC numbers loaded.")
    return numbers


# === DEDUPLICATION LOGIC ==========================================================

def pick_primary_phone(row) -> str:
    """
    Return the FIRST non-empty phone for a row, in this priority:
       1) Mobile
       2) Landline
       3) AltNumber

    Used to decide which phone to group on for duplicate detection.
    """
    for col in ["Mobile", "Landline", "AltNumber"]:
        if col in row and isinstance(row[col], str) and row[col]:
            return row[col]
    return ""


def merge_group(group: pd.DataFrame, notes_merge: bool):
    """
    Merge a group of duplicate rows into one "master" row.

    - Picks as master the row with the most non-empty fields.
    - For every other row in the group:
        * fill empty fields on master
        * merge phone numbers (avoiding duplicates)
        * optionally merge Notes
    - Builds a DataFrame of 'duplicate rows' with an added column __MergedInto.

    Returns:
      master:   a Series representing the merged row
      dup_df:   a DataFrame of duplicates (all non-master rows)
    """
    # 1) Choose master row index = row with most non-empty values
    non_empty_counts = group.replace("", pd.NA).notna().sum(axis=1)
    master_idx = non_empty_counts.idxmax()
    master = group.loc[master_idx].copy()

    duplicate_rows = []

    # 2) Loop through each row in the group and merge into master
    for idx, row in group.iterrows():
        if idx == master_idx:
            continue  # skip the master itself

        dup_row = row.copy()  # we'll store this as a "duplicate/archive" row

        # --- Merge non-phone, non-Notes columns ---
        for col in group.columns:
            # Ignore internal columns like __source_file, __row_id, etc.
            if col.startswith("__"):
                continue
            # We'll handle phones & Notes separately
            if col in PHONE_COLUMNS or col == "Notes":
                continue

            master_val = master.get(col, "")
            row_val = row.get(col, "")

            # If master is empty but duplicate row has a value -> keep it
            if (not master_val) and row_val:
                master[col] = row_val

        # --- Merge phone numbers ---
        for col in PHONE_COLUMNS:
            row_phone = row.get(col, "")
            if not row_phone:
                continue

            # Collect what numbers master already contains
            existing = {str(master.get(c, "")) for c in PHONE_COLUMNS if master.get(c, "")}
            if row_phone in existing:
                continue  # already have this number

            # Try to put the number into the first empty phone column
            placed = False
            for target_col in PHONE_COLUMNS:
                if not master.get(target_col, ""):
                    master[target_col] = row_phone
                    placed = True
                    break
            # If all three phone columns are used, extra numbers are ignored (MVP decision)

        # --- Merge notes ---
        if notes_merge and "Notes" in group.columns:
            master_notes = master.get("Notes", "")
            row_notes = row.get("Notes", "")
            if row_notes:
                if master_notes:
                    # Avoid duplicating text if it already contains it
                    if row_notes not in master_notes:
                        master["Notes"] = master_notes + " | " + row_notes
                else:
                    master["Notes"] = row_notes

        # Mark this duplicate row with pointer to master index
        dup_row["__MergedInto"] = master_idx
        duplicate_rows.append(dup_row)

    # Build DataFrame of duplicates (may be empty if group had only one row)
    dup_df = (
        pd.DataFrame(duplicate_rows)
        if duplicate_rows
        else pd.DataFrame(columns=group.columns.tolist() + ["__MergedInto"])
    )
    return master, dup_df
#---------------------------------------------------------------------TODO

def dedupe(df: pd.DataFrame, config: dict):
    """
    Deduplicate the combined DataFrame using two passes:

    PASS 1: Group by primary phone number (if present).
            Each group of 2+ rows is merged into one master.

    PASS 2: For any rows NOT used in pass 1:
            Group by (Address1 + Postcode).
            Each group of 2+ rows is merged into one master.

    Anything not part of any duplicate group remains as-is.

    Returns:
      cleaned_df: DataFrame with one row per real lead (after merges)
      duplicates_df: DataFrame of all archived duplicate rows
    """
    notes_merge = config.get("merge_notes", True)

    # We will store ONLY dicts in cleaned_rows (no Series) to keep pandas happy.
    cleaned_rows = []         # master rows + unique rows as dicts
    duplicate_rows_all = []   # all duplicate/archive rows

    df = df.reset_index(drop=True)
    df["__row_id"] = df.index  # keep an internal ID for reference

    used_indices = set()  # indices already merged into a group

    # --- PASS 1: dedupe by primary phone number -----------------------------
    phone_to_indices = {}
    for idx, row in df.iterrows():
        phone = pick_primary_phone(row)
        if not phone:
            continue
        phone_to_indices.setdefault(phone, []).append(idx)

    for phone, idxs in phone_to_indices.items():
        if len(idxs) < 2:
            continue  # this phone is unique

        group = df.loc[idxs]
        master, dup_df = merge_group(group, notes_merge)

        # IMPORTANT: convert master (Series) to dict before storing
        cleaned_rows.append(master.to_dict())

        if not dup_df.empty:
            duplicate_rows_all.append(dup_df)
        used_indices.update(idxs)

    # --- PASS 2: dedupe by (Address1 + Postcode) for rows not yet used ------
    addr_groups = {}
    for idx, row in df.iterrows():
        if idx in used_indices:
            continue  # already merged by phone

        postcode = tidy_whitespace(row.get("Postcode", ""))
        addr1 = tidy_whitespace(row.get("Address1", ""))
        if not postcode or not addr1:
            continue  # can't group without both

        key = (addr1.lower(), postcode.replace(" ", "").upper())
        addr_groups.setdefault(key, []).append(idx)

    for key, idxs in addr_groups.items():
        if len(idxs) < 2:
            continue  # no dupes here

        group = df.loc[idxs]
        master, dup_df = merge_group(group, notes_merge)

        # Again, always convert master Series -> dict
        cleaned_rows.append(master.to_dict())

        if not dup_df.empty:
            duplicate_rows_all.append(dup_df)
        used_indices.update(idxs)

    # --- Remaining rows: never part of any duplicate group ------------------
    remaining_indices = [i for i in df.index if i not in used_indices]
    remaining = df.loc[remaining_indices]

    # remaining.to_dict(orient="records") already returns a list of dicts
    cleaned_rows.extend(remaining.to_dict(orient="records"))

    # --- Build final DataFrames ---------------------------------------------
    cleaned_df = pd.DataFrame(cleaned_rows)

    duplicates_df = (
        pd.concat(duplicate_rows_all, ignore_index=True)
        if duplicate_rows_all
        else pd.DataFrame(columns=df.columns.tolist() + ["__MergedInto"])
    )

    return cleaned_df, duplicates_df

# === INVALID + DNC SPLITTING ======================================================

def split_invalid_and_dnc(df: pd.DataFrame, dnc_numbers: set, config: dict):
    """
    Split the de-duplicated DataFrame into:

      - cleaned_df: final good leads
      - invalid_df: rows with NO phone at all
      - dnc_df: rows where any phone matches the DNC set

    Note:
      DNC is checked ONLY AFTER we've confirmed the row has at least one phone.
    """
    # Ensure all phone columns exist
    for col in PHONE_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # --- INVALID: no phone in ANY phone column ---------------------------------
    # Build a mask for "no non-empty phone in any phone column"
    mask_no_phone = ~(
        df[PHONE_COLUMNS]
        .astype(str)
        .apply(lambda s: s.str.len() > 0)
        .any(axis=1)
    )

    invalid_df = df[mask_no_phone].copy()
    remaining = df[~mask_no_phone].copy()

    # --- DNC: any phone in DNC set --------------------------------------------
    def row_is_dnc(row) -> bool:
        """
        Return True if ANY phone field in the row is in the DNC set.
        """
        for col in PHONE_COLUMNS:
            num = str(row.get(col, "")).strip()
            if num and num in dnc_numbers:
                return True
        return False

    dnc_mask = remaining.apply(row_is_dnc, axis=1)
    dnc_df = remaining[dnc_mask].copy()
    cleaned_df = remaining[~dnc_mask].copy()

    return cleaned_df, invalid_df, dnc_df


# === OUTPUT / REPORTING ===========================================================

def save_output(
    df_cleaned: pd.DataFrame,
    df_dups: pd.DataFrame,
    df_invalid: pd.DataFrame,
    df_dnc: pd.DataFrame,
    config: dict,
    start_time: datetime,
    input_files: dict,
):
    """
    Save all outputs and generate a summary report & log.

    - Cleaned data -> /output/cleaned as ODS/XLSX (based on config)
    - Duplicates   -> /output/duplicates as CSV
    - Invalid      -> /output/invalid as CSV
    - DNC hits     -> /output/dnc_hits as CSV
    - Summary text -> /output/reports/report_*.txt
    - Simple log   -> /logs/log_*.txt

    Returns:
      Path to the summary report file.
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Decide which formats to use for the main cleaned file
    fmt = config.get("output_format", "ods").lower()
    if fmt == "both":
        formats = ["ods", "xlsx"]
    elif fmt in ("ods", "xlsx"):
        formats = [fmt]
    else:
        formats = ["ods"]  # safe default

    base_name = f"cleaned_{ts}"

    # --- Save cleaned data in chosen formats ---------------------------------
    for f in formats:
        if f == "ods":
            out_path = CLEANED_DIR / f"{base_name}.ods"
            with pd.ExcelWriter(out_path, engine="odf") as writer:
                df_cleaned.to_excel(writer, index=False, sheet_name="Cleaned")
        elif f == "xlsx":
            out_path = CLEANED_DIR / f"{base_name}.xlsx"
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                df_cleaned.to_excel(writer, index=False, sheet_name="Cleaned")

    # --- Save duplicates / invalid / DNC as CSV ------------------------------
    if not df_dups.empty:
        df_dups.to_csv(DUP_DIR / f"duplicates_{ts}.csv", index=False)
    if not df_invalid.empty:
        df_invalid.to_csv(INVALID_DIR / f"invalid_{ts}.csv", index=False)
    if not df_dnc.empty:
        df_dnc.to_csv(DNC_HITS_DIR / f"dnc_hits_{ts}.csv", index=False)

    # --- Generate summary report --------------------------------------------
    report_path = REPORTS_DIR / f"report_{ts}.txt"
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    total_in = sum(len(df) for df in input_files.values())
    total_cleaned = len(df_cleaned)
    total_dups = len(df_dups)
    total_invalid = len(df_invalid)
    total_dnc = len(df_dnc)

    with open(report_path, "w", encoding="utf-8") as rep:
        rep.write("Portable Data Cleaner - MVP\n")
        rep.write(f"Run started: {start_time}\n")
        rep.write(f"Run finished: {end_time}\n")
        rep.write(f"Duration (seconds): {duration:.2f}\n\n")

        rep.write("Input files:\n")
        for name, df in input_files.items():
            rep.write(f" - {name}: {len(df)} rows\n")
        rep.write("\n")

        rep.write(f"Total input rows: {total_in}\n")
        rep.write(f"Total cleaned rows (final): {total_cleaned}\n")
        rep.write(f"Total duplicate rows (archived): {total_dups}\n")
        rep.write(f"Total invalid rows (no phone): {total_invalid}\n")
        rep.write(f"Total DNC hits: {total_dnc}\n")

    # --- Simple log (machine-readable-ish) -----------------------------------
    log_path = LOG_DIR / f"log_{ts}.txt"
    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"Run started: {start_time}\n")
        log.write(f"Run finished: {end_time}\n")
        log.write(f"Duration: {duration:.2f}s\n")
        log.write(f"Input files: {', '.join(input_files.keys())}\n")
        log.write(
            f"Cleaned rows: {total_cleaned}, "
            f"Duplicates: {total_dups}, "
            f"Invalid: {total_invalid}, "
            f"DNC: {total_dnc}\n"
        )

    return report_path


# === MAIN ENTRYPOINT ==============================================================

def main():
    """
    Main entrypoint for the script.

    This wires all the steps together:

    1. Ensure folders exist
    2. Load config
    3. Find input files
    4. Read + normalise + combine them
    5. Normalise text/postcodes/phones
    6. Load DNC set
    7. Deduplicate (phone, then address+postcode)
    8. Split invalid and DNC
    9. Save outputs + report + log
    """
    print("=== Portable Data Cleaner (MVP) ===")
    start_time = datetime.now()

    ensure_directories()
    config = load_config()

    print(f"Base directory: {BASE_DIR}")
    print(f"Using output format: {config.get('output_format', 'ods')}")

    # --- Locate input files --------------------------------------------------
    input_paths = list_input_files()
    if not input_paths:
        print(f"No input files found in: {INPUT_DIR}")
        print("Drop CSV/XLSX/ODS files into the input folder and run again.")
        return

    print(f"Found {len(input_paths)} input file(s):")
    for f in input_paths:
        print(f" - {f.name}")

    # --- Read & header-normalise all input files -----------------------------
    input_dfs = {}   # keeps each individual file's DataFrame (for reporting)
    frames = []      # combined list for concatenation

    for path in input_paths:
        try:
            df = read_input_file(path)
        except Exception as e:
            print(f"⚠ Failed to read {path.name}: {e}")
            continue

        df = normalise_headers(df)
        input_dfs[path.name] = df
        frames.append(df)

    if not frames:
        print("No input data could be loaded. Exiting.")
        return

    # Combine all rows into one big DataFrame
    combined = pd.concat(frames, ignore_index=True)

    # --- Normalise text, postcodes, phones, etc. -----------------------------
    combined = apply_normalisation(combined, config)

    # --- Load DNC numbers ----------------------------------------------------
    dnc_numbers = load_dnc_numbers()

    # --- Deduplicate ---------------------------------------------------------
    print("Deduplicating records (phone, then address+postcode)...")
    deduped_df, dup_df = dedupe(combined, config)

    # --- Split invalid + DNC -------------------------------------------------
    print("Splitting invalid (no phone) and DNC hits...")
    cleaned_df, invalid_df, dnc_df = split_invalid_and_dnc(deduped_df, dnc_numbers, config)

    # --- Save all outputs ----------------------------------------------------
    print("Saving outputs...")
    report_path = save_output(
        cleaned_df,
        dup_df,
        invalid_df,
        dnc_df,
        config,
        start_time,
        input_dfs,
    )

    # --- Final console summary ----------------------------------------------
    print("\nRun complete.")
    print(f"Cleaned rows: {len(cleaned_df)}")
    print(f"Duplicates archived: {len(dup_df)}")
    print(f"Invalid (no phone): {len(invalid_df)}")
    print(f"DNC hits: {len(dnc_df)}")
    print(f"Report created at: {report_path}")


if __name__ == "__main__":
    main()
