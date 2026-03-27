"""
anonymize_xxx.py — Owner-Only Anonymization Script
===================================================
A stricter anonymization variant:

  - The FILE OWNER (the person who submitted the data) gets a consistent
    USER_N / PH_NUMBER_N alias across all files.
  - EVERY OTHER name or phone number in the message content is replaced
    with a fixed redaction mask:  XXXXXXXXX  (names) / XXXXXXXXXXX (phones).

This gives third parties (senders, recipients, agents) maximum privacy —
they are not individually tracked at all.

Project layout:
    Project/
    ├── data/              ← SMS export files (.csv or .xlsx)
    ├── anonymize_xxx.py   ← this script
    └── output_xxx/        ← created automatically
        ├── <file>_anonymized.csv / .xlsx
        └── owner_map.json   (owner alias ↔ original name/phone)

Usage:
    python anonymize_xxx.py

Dependencies:
    pip install pandas openpyxl
    pip install chardet          # optional but improves encoding detection
"""

import re
import json
import unicodedata
from collections import Counter
from pathlib import Path

import pandas as pd


# ─── Paths ────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
DATA_DIR   = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output_xxx"
MAP_FILE   = OUTPUT_DIR / "owner_map.json"

# SMS Exporter prepends 3 metadata lines before the real header row
CSV_SKIP = 3

# Redaction masks for third-party names and numbers
MASK_NAME  = "XXXXXXXXX"
MASK_PHONE = "XXXXXXXXXXX"

# French → English column name map
COLUMN_MAP = {
    "Heure":     "Time",
    "Téléphone": "Phone",
    "Contenu":   "Content",
}

# Any of these must be present for the read to be considered valid
EXPECTED_COLS = {"Content", "Contenu", "Date"}

# Fallback encoding probe order
ENCODINGS = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]


# ─── Regex patterns ───────────────────────────────────────────────────────────

# Cameroonian phone numbers — 9 digits starting with 6 or 2, optional 237 prefix
PHONE_RE = re.compile(r"\b(237)?(6\d{8}|2\d{8})\b")

# MTN MoMo: "Full Name (237XXXXXXXXX)"
MTN_NAME_NUM_RE = re.compile(
    r"(\b[A-ZÀ-Ýa-zà-ý][A-ZÀ-Ýa-zà-ý]+"
    r"(?:\s+[A-ZÀ-Ýa-zà-ý&][A-ZÀ-Ýa-zà-ý&]+)*)"
    r"\s+\(237(\d{9})\s*\)"   # \s* tolerates a trailing space before )
)

# Orange Money: "<number> <Name Words>" before a keyword / punctuation
OM_NUM_NAME_RE = re.compile(
    r"\b((?:237)?(?:6\d{8}|2\d{8}))\s+"
    r"((?:[A-ZÀ-Ýa-zà-ý&][A-ZÀ-Ýa-zà-ý&]+)"
    r"(?:\s+(?!to\b|vers\b|avec\b|reussi\b|Informations\b)"
    r"[A-ZÀ-Ýa-zà-ý&][A-ZÀ-Ýa-zà-ý&]+){0,4})"
    r"(?=\s+(?:to|vers|avec|reussi|Informations)|\s*[.\n,])"
)

# Words that may prefix a name in a sentence but are not part of the name
NAME_STOPWORDS = {
    "from", "to", "of", "by",
    "de", "du", "par", "vers", "avec", "le", "la", "les", "un", "une",
    "your", "xaf", "fcfa",
}


# ─── Text helpers ─────────────────────────────────────────────────────────────

def strip_accents(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def normalise(s: str) -> str:
    """Lowercase + strip accents + collapse whitespace — for consistent matching."""
    return re.sub(r"\s+", " ", strip_accents(str(s)).strip().lower())


def clean_name(raw: str) -> str:
    """Strip leading prepositions/stopwords absorbed by the regex."""
    words = raw.split()
    while words and words[0].lower() in NAME_STOPWORDS:
        words = words[1:]
    return " ".join(words)


def bare_phone(raw: str) -> str:
    """Return bare 9-digit number, stripping the 237 country prefix if present."""
    s = str(raw).strip().replace(" ", "")
    return s[3:] if s.startswith("237") and len(s) == 12 else s


# ─── Owner detection ──────────────────────────────────────────────────────────

def detect_owner_phone(df: pd.DataFrame) -> str | None:
    """
    Identify the file owner's phone number.

    The owner's number is the single most-frequent phone number across all
    message content — it appears in every balance confirmation, rechargement,
    transfer, etc.  Third-party numbers appear far less often.
    """
    counter: Counter = Counter()
    for text in df.get("Content", pd.Series(dtype=str)).dropna():
        for m in PHONE_RE.finditer(str(text)):
            counter[m.group(2)] += 1   # bare 9-digit
    return counter.most_common(1)[0][0] if counter else None


def detect_owner_name(df: pd.DataFrame, owner_phone: str | None) -> str | None:
    """
    Identify the file owner's name.

    Strategy (in priority order):
    1. MTN format — messages like "You JOEL XAVIER DEJON FAH (237653415650) have..."
       where the owner's number appears alongside their name.
    2. Orange Money format — messages like "to 656997810 DEJON FAH. Informations..."
       where the owner's number appears before their name.
    3. Fall back to None if not found.
    """
    if not owner_phone:
        return None

    for text in df.get("Content", pd.Series(dtype=str)).dropna():
        text = str(text)

        # MTN pattern: owner name immediately precedes their own number in parentheses
        for m in MTN_NAME_NUM_RE.finditer(text):
            if bare_phone("237" + m.group(2)) == owner_phone:
                name = clean_name(m.group(1))
                if name:
                    return name

        # OM pattern: owner number is followed by their name
        for m in OM_NUM_NAME_RE.finditer(text):
            if bare_phone(m.group(1)) == owner_phone:
                name = clean_name(m.group(2))
                if name:
                    return name

    return None


# ─── Per-file anonymization ───────────────────────────────────────────────────

def anonymize_text(text: str,
                   owner_phone: str,
                   owner_name_norm: str | None,
                   phone_alias: str,
                   name_alias: str) -> str:
    """
    Replace identifiers in a single message:
      - Owner phone   → phone_alias  (e.g. PH_NUMBER_1)
      - Owner name    → name_alias   (e.g. USER_1)
      - Any other phone → XXXXXXXXXXX
      - Any other name  → XXXXXXXXX
    """
    if not isinstance(text, str):
        return text

    # ── Step 1: MTN "Name (237XXXXXXXXX)" pairs ──────────────────────────────
    def repl_mtn(m):
        raw_name = m.group(1)
        phone    = bare_phone("237" + m.group(2))

        # Strip leading stopwords; reconstruct the prefix as literal text
        words = raw_name.split()
        clean_words = words[:]
        while clean_words and clean_words[0].lower() in NAME_STOPWORDS:
            clean_words = clean_words[1:]
        clean = " ".join(clean_words)
        prefix_text = raw_name[: len(raw_name) - len(clean)].rstrip()
        prefix_out  = (prefix_text + " ") if prefix_text else ""

        if phone == owner_phone:
            n_out = name_alias
            p_out = phone_alias
        else:
            n_out = MASK_NAME
            p_out = MASK_PHONE

        return f"{prefix_out}{n_out} ({p_out})"

    text = MTN_NAME_NUM_RE.sub(repl_mtn, text)

    # ── Step 2: Orange Money "<number> <Name>" pairs ─────────────────────────
    def repl_om(m):
        phone = bare_phone(m.group(1))
        name  = clean_name(m.group(2))  # already stripped

        if phone == owner_phone:
            p_out = phone_alias
            n_out = name_alias
        else:
            p_out = MASK_PHONE
            n_out = MASK_NAME if name else ""

        return f"{p_out} {n_out}".rstrip()

    text = OM_NUM_NAME_RE.sub(repl_om, text)

    # ── Step 3: remaining bare phone numbers ─────────────────────────────────
    def repl_phone(m):
        phone = m.group(2)
        return phone_alias if phone == owner_phone else MASK_PHONE

    text = PHONE_RE.sub(repl_phone, text)

    # ── Step 4: owner name sweep (case-insensitive) ───────────────────────────
    # Catches any remaining mentions of the owner's name in plain prose
    if owner_name_norm and len(owner_name_norm) >= 4:
        text = re.sub(re.escape(owner_name_norm), name_alias,
                      text, flags=re.IGNORECASE)

    return text


# ─── File I/O (reused from anonymize.py) ──────────────────────────────────────

def _detect_encoding(path: Path) -> str:
    try:
        import chardet
        raw = path.read_bytes()[:8192]
        result = chardet.detect(raw)
        if result["encoding"] and result["confidence"] > 0.7:
            return result["encoding"]
    except ImportError:
        pass
    for enc in ENCODINGS:
        try:
            with open(path, encoding=enc) as f:
                f.read(4096)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin-1"


def _read_csv_robust(path: Path, encoding: str, skiprows: int) -> pd.DataFrame:
    """
    Line-by-line CSV reader that handles unquoted commas inside the Content field.
    The SMS Exporter format always has exactly 7 columns; commas in Content are
    disambiguated by splitting from the right for the last (Type) column.
    """
    with open(path, encoding=encoding, errors="replace") as f:
        all_lines = f.readlines()

    data_lines = all_lines[skiprows:]
    if not data_lines:
        raise pd.errors.EmptyDataError("No data after skipping rows")

    header_line = data_lines[0].rstrip("\r\n")
    sep         = "\t" if "\t" in header_line else ","
    header      = [h.strip().strip('"') for h in header_line.split(sep)]
    ncols       = len(header)

    rows = []
    for line in data_lines[1:]:
        line = line.rstrip("\r\n")
        if not line.strip():
            continue
        if sep == "\t":
            parts = line.split("\t")
        else:
            parts = line.split(",", ncols - 2)
            if len(parts) == ncols - 1:
                lc = parts[-1].rfind(",")
                if lc != -1:
                    parts = parts[:-1] + [parts[-1][:lc], parts[-1][lc + 1:]]
        if len(parts) < ncols:
            parts += [""] * (ncols - len(parts))
        else:
            parts = parts[:ncols]
        rows.append([p.strip().strip('"') for p in parts])

    return pd.DataFrame(rows, columns=header)


def load_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        encoding = _detect_encoding(path)
        for skip in (CSV_SKIP, 0):
            try:
                df = _read_csv_robust(path, encoding, skip)
            except pd.errors.EmptyDataError:
                continue
            df.rename(columns=COLUMN_MAP, inplace=True)
            if EXPECTED_COLS.intersection(df.columns):
                break
        else:
            raise ValueError(
                f"Could not find expected columns in '{path.name}'. "
                f"Columns: {list(df.columns)}"
            )
    elif ext in (".xlsx", ".xls"):
        for skip in (CSV_SKIP, 0):
            df = pd.read_excel(path, skiprows=skip, header=0, dtype=str)
            df.rename(columns=COLUMN_MAP, inplace=True)
            if EXPECTED_COLS.intersection(df.columns):
                break
        else:
            raise ValueError(f"Could not find expected columns in '{path.name}'")
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    df = df.where(df.notna(), other=None)
    return df


def save_file(df: pd.DataFrame, path: Path):
    ext = path.suffix.lower()
    if ext == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
    elif ext in (".xlsx", ".xls"):
        df.to_excel(path, index=False)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


# ─── Main pipeline ────────────────────────────────────────────────────────────

def process_files():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    supported = {".csv", ".xlsx", ".xls"}
    files = sorted(
        f for f in DATA_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in supported
    )

    if not files:
        print(f"[!] No supported files found in '{DATA_DIR}'. Nothing to do.")
        return

    print(f"[→] Found {len(files)} file(s) in '{DATA_DIR}'")
    print()

    # ── Global owner registry ─────────────────────────────────────────────────
    # Maps normalised owner name → (USER_N, PH_NUMBER_N)
    # so the same person gets the same alias even if they submitted two files
    # (one OM, one MTN).
    owner_registry: dict[str, tuple[str, str]] = {}
    user_counter   = 0
    phone_counter  = 0

    def get_or_create_alias(name_norm: str) -> tuple[str, str]:
        nonlocal user_counter, phone_counter
        if name_norm not in owner_registry:
            user_counter  += 1
            phone_counter += 1
            owner_registry[name_norm] = (
                f"USER_{user_counter}",
                f"PH_NUMBER_{phone_counter}",
            )
        return owner_registry[name_norm]

    # ── Process each file independently ──────────────────────────────────────
    print("─── Processing files ───")
    owner_map_records = []

    for filepath in files:
        print(f"    {filepath.name} ...", end=" ", flush=True)

        # Load
        try:
            df = load_file(filepath)
        except Exception as e:
            print(f"ERROR loading — {e}")
            continue

        if "Content" not in df.columns:
            print("WARN — 'Content' column not found, skipping")
            continue

        # Detect owner from message content
        owner_phone     = detect_owner_phone(df)
        owner_name_raw  = detect_owner_name(df, owner_phone)

        # Fall back to filename if content detection fails
        if not owner_name_raw:
            stem = filepath.stem
            sep  = " - " if " - " in stem else ("_-_" if "_-_" in stem else None)
            if sep:
                owner_name_raw = stem.rsplit(sep, 1)[1].replace("_", " ").strip()

        owner_name_norm = normalise(owner_name_raw) if owner_name_raw else None

        # Assign aliases
        if owner_name_norm:
            name_alias, phone_alias = get_or_create_alias(owner_name_norm)
        else:
            # Can't identify owner — still produce the file, use a generic alias
            user_counter  += 1
            phone_counter += 1
            name_alias  = f"USER_{user_counter}"
            phone_alias = f"PH_NUMBER_{phone_counter}"
            owner_name_norm = f"unknown_{user_counter}"

        print(f"owner={name_alias} ({owner_name_raw or 'unknown'}), "
              f"phone={owner_phone or 'unknown'}", end=" ... ", flush=True)

        # Anonymize Content column
        anon = df.copy()
        anon["Content"] = anon["Content"].apply(
            lambda t: anonymize_text(
                t,
                owner_phone     = owner_phone or "",
                owner_name_norm = owner_name_norm,
                phone_alias     = phone_alias,
                name_alias      = name_alias,
            )
        )

        # Build output filename — replace owner name portion with alias
        stem      = filepath.stem
        sep_token = " - " if " - " in stem else ("_-_" if "_-_" in stem else None)
        if sep_token:
            prefix, _ = stem.rsplit(sep_token, 1)
            new_stem  = prefix + sep_token + name_alias
        else:
            new_stem = stem + "_" + name_alias

        # Dedup guard — two files from the same owner get _1, _2, ...
        candidate = OUTPUT_DIR / (new_stem + "_anonymized" + filepath.suffix)
        out_path  = candidate
        ctr       = 1
        while out_path.exists():
            out_path = OUTPUT_DIR / (new_stem + f"_{ctr}_anonymized" + filepath.suffix)
            ctr += 1

        try:
            save_file(anon, out_path)
            print(f"OK → {out_path.name}")
        except Exception as e:
            print(f"ERROR saving — {e}")
            continue

        # Track for owner map
        owner_map_records.append({
            "user_alias":    name_alias,
            "phone_alias":   phone_alias,
            "original_name": owner_name_raw,
            "original_phone": owner_phone,
            "source_file":   filepath.name,
            "output_file":   out_path.name,
        })

    # ── Write owner map ───────────────────────────────────────────────────────
    print()
    print("─── Writing owner map ───")
    with open(MAP_FILE, "w", encoding="utf-8") as f:
        json.dump({"owner_map": owner_map_records}, f, ensure_ascii=False, indent=2)
    print(f"    Saved  →  {MAP_FILE.name}  ({len(owner_map_records)} record(s))")
    print()
    print(f"✓ Done. Outputs are in:  {OUTPUT_DIR}")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    process_files()