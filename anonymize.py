"""
anonymize.py — Mobile Money SMS Anonymization Script
=====================================================
Scans all .csv and .xlsx files in ./data/ and produces:
  - Anonymized copies in ./output/  (phone numbers -> PH_NUMBER_N, names -> USER_N)
  - A lookup table ./output/identity_map.json linking aliases to originals

Supports both Orange Money and MTN MoMo SMS formats (English & French).
IDs are consistent: same name/number always gets the same alias across all files.
When a name and number appear together they share the same N (USER_2 = PH_NUMBER_2).

Project layout:
    Project/
    ├── data/            ← drop SMS export files here (.csv or .xlsx)
    ├── anonymize.py     ← this script
    └── output/          ← created automatically
        ├── <file>_anonymized.csv  (or .xlsx)
        └── identity_map.json

Usage:
    python anonymize.py

Dependencies:
    pip install pandas openpyxl chardet
"""

import re
import json
import unicodedata
import pandas as pd
from pathlib import Path


# ─── Paths ────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
DATA_DIR   = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output"
MAP_FILE   = OUTPUT_DIR / "identity_map.json"

# SMS Exporter prepends 3 metadata lines before the real header row
CSV_SKIP = 3

# Words that can appear before a name in a sentence — never part of the name
NAME_STOPWORDS = {
    "from", "to", "of", "by",
    "de", "du", "par", "vers", "avec", "le", "la", "les", "un", "une",
    "your", "xaf", "fcfa",
}

# French → English column name mapping (normalised after loading)
COLUMN_MAP = {
    "Heure":     "Time",
    "Téléphone": "Phone",
    "Contenu":   "Content",
}

# Expected column names in either language — used to validate a read attempt
EXPECTED_COLS = {"Content", "Contenu", "Date"}

# Encodings to try in order when opening a CSV
ENCODINGS = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]


# ─── Regex patterns ───────────────────────────────────────────────────────────

# Cameroonian phone numbers: 9 digits starting with 6 or 2, optional 237 prefix
PHONE_RE = re.compile(r"\b(237)?(6\d{8}|2\d{8})\b")

# MTN MoMo: "Name (237XXXXXXXXX)"  — any casing, leading prepositions absorbed
# and stripped later in repl_mtn()
MTN_NAME_NUM_RE = re.compile(
    r"(\b[A-ZÀ-Ýa-zà-ý][A-ZÀ-Ýa-zà-ý]+"
    r"(?:\s+[A-ZÀ-Ýa-zà-ý&][A-ZÀ-Ýa-zà-ý&]+)*)"
    r"\s+\(237(\d{9})\)"
)

# Orange Money: "<number> <Name Words>" before a keyword or punctuation
OM_NUM_NAME_RE = re.compile(
    r"\b((?:237)?(?:6\d{8}|2\d{8}))\s+"
    r"((?:[A-ZÀ-Ýa-zà-ý&][A-ZÀ-Ýa-zà-ý&]+)"
    r"(?:\s+(?!to\b|vers\b|avec\b|reussi\b|Informations\b)"
    r"[A-ZÀ-Ýa-zà-ý&][A-ZÀ-Ýa-zà-ý&]+){0,4})"
    r"(?=\s+(?:to|vers|avec|reussi|Informations)|\s*[.\n,])"
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def clean_name(raw: str) -> str:
    """Strip leading prepositions/stopwords from a captured name fragment."""
    words = raw.split()
    while words and words[0].lower() in NAME_STOPWORDS:
        words = words[1:]
    return " ".join(words)


# ─── Identity registry ────────────────────────────────────────────────────────

class IdentityRegistry:
    """
    Bidirectional, consistent mapping between real identifiers and aliases.

    Rules:
    - Every unique phone number gets exactly one PH_NUMBER_N.
    - Every unique person name gets exactly one USER_N.
    - When a name and number co-occur, they are linked (same N index).
    - Consistency is guaranteed across all files in a single run.
    """

    def __init__(self):
        self._phone_to_id   = {}   # "656997810" -> "PH_NUMBER_3"
        self._name_to_id    = {}   # "dejon fah" -> "USER_3"
        self._phone_to_user = {}   # "656997810" -> "USER_3"
        self._user_to_phone = {}   # "USER_3"    -> "PH_NUMBER_3"
        self._ph_ctr  = 0
        self._usr_ctr = 0

    def _bare(self, raw: str) -> str:
        s = str(raw).strip().replace(" ", "")
        return s[3:] if s.startswith("237") and len(s) == 12 else s

    @staticmethod
    def _strip_accents(s: str) -> str:
        """Decompose accented chars and drop the accent marks (e.g. é → e)."""
        return (unicodedata.normalize("NFKD", s)
                .encode("ascii", "ignore").decode())

    def _norm(self, raw: str) -> str:
        """Lowercase, collapse whitespace, strip accents for consistent matching."""
        return re.sub(r"\s+", " ", self._strip_accents(str(raw)).strip().lower())

    def _new_ph(self) -> str:
        self._ph_ctr += 1
        return f"PH_NUMBER_{self._ph_ctr}"

    def _new_usr(self) -> str:
        self._usr_ctr += 1
        return f"USER_{self._usr_ctr}"

    def register_pair(self, raw_name: str, raw_phone: str):
        name  = self._norm(clean_name(raw_name))
        phone = self._bare(raw_phone)
        if not name:
            self.get_phone_id(raw_phone)
            return
        ph_id  = self._phone_to_id.get(phone)
        usr_id = self._name_to_id.get(name)
        if ph_id is None and usr_id is None:
            usr_id = self._new_usr(); ph_id = self._new_ph()
            self._phone_to_id[phone]    = ph_id
            self._name_to_id[name]      = usr_id
            self._phone_to_user[phone]  = usr_id
            self._user_to_phone[usr_id] = ph_id
        elif ph_id is not None and usr_id is None:
            usr_id = self._phone_to_user.get(phone, self._new_usr())
            self._name_to_id[name]      = usr_id
            self._phone_to_user[phone]  = usr_id
            self._user_to_phone[usr_id] = ph_id
        elif ph_id is None and usr_id is not None:
            ph_id = self._new_ph()
            self._phone_to_id[phone]    = ph_id
            self._phone_to_user[phone]  = usr_id
            self._user_to_phone[usr_id] = ph_id

    def get_phone_id(self, raw: str) -> str:
        phone = self._bare(raw)
        if phone not in self._phone_to_id:
            self._phone_to_id[phone] = self._new_ph()
        return self._phone_to_id[phone]

    def get_name_id(self, raw: str) -> str:
        name = self._norm(clean_name(raw))
        if not name:
            return ""
        if name not in self._name_to_id:
            self._name_to_id[name] = self._new_usr()
        return self._name_to_id[name]

    def to_dict(self) -> dict:
        id_to_phone = {v: k for k, v in self._phone_to_id.items()}
        id_to_name  = {v: k for k, v in self._name_to_id.items()}
        records = []
        seen_users = set()
        for usr_id, ph_id in self._user_to_phone.items():
            records.append({"user_alias": usr_id, "phone_alias": ph_id,
                            "original_name": id_to_name.get(usr_id),
                            "original_phone": id_to_phone.get(ph_id)})
            seen_users.add(usr_id)
        for name, usr_id in self._name_to_id.items():
            if usr_id not in seen_users:
                records.append({"user_alias": usr_id, "phone_alias": None,
                                "original_name": name, "original_phone": None})
                seen_users.add(usr_id)
        for phone, ph_id in self._phone_to_id.items():
            if phone not in self._phone_to_user:
                records.append({"user_alias": None, "phone_alias": ph_id,
                                "original_name": None, "original_phone": phone})
        records.sort(key=lambda r: int(r["user_alias"].split("_")[1])
                     if r.get("user_alias") else 99999)
        return {"identity_map": records}


# ─── Pass 1 — identity extraction ─────────────────────────────────────────────

def extract_identities(text: str, registry: IdentityRegistry):
    if not isinstance(text, str):
        return
    for m in MTN_NAME_NUM_RE.finditer(text):
        registry.register_pair(m.group(1), "237" + m.group(2))
    for m in OM_NUM_NAME_RE.finditer(text):
        registry.register_pair(m.group(2), m.group(1))


# ─── Pass 2 — anonymization ───────────────────────────────────────────────────

def anonymize_text(text: str, registry: IdentityRegistry) -> str:
    if not isinstance(text, str):
        return text

    # Step 1 — MTN "Name (237XXXXXXXXX)" → "from USER_N (PH_NUMBER_N)"
    def repl_mtn(m):
        raw_name    = m.group(1)
        words       = raw_name.split()
        clean_words = words[:]
        while clean_words and clean_words[0].lower() in NAME_STOPWORDS:
            clean_words = clean_words[1:]
        clean_name_str  = " ".join(clean_words)
        stripped_prefix = raw_name[: len(raw_name) - len(clean_name_str)].rstrip()
        prefix_str      = (stripped_prefix + " ") if stripped_prefix else ""
        name_alias      = registry.get_name_id(clean_name_str)
        phone_alias     = registry.get_phone_id("237" + m.group(2))
        return f"{prefix_str}{name_alias} ({phone_alias})" if name_alias else f"{prefix_str}({phone_alias})"

    text = MTN_NAME_NUM_RE.sub(repl_mtn, text)

    # Step 2 — Orange Money "<number> <n>" → "PH_NUMBER_N USER_N"
    def repl_om(m):
        phone_alias = registry.get_phone_id(m.group(1))
        name_alias  = registry.get_name_id(m.group(2))
        return f"{phone_alias} {name_alias}" if name_alias else phone_alias

    text = OM_NUM_NAME_RE.sub(repl_om, text)

    # Step 3 — remaining bare phone numbers
    text = PHONE_RE.sub(lambda m: registry.get_phone_id(m.group(2)), text)

    # Step 4 — case-insensitive sweep for any known names still in the text
    for name_norm, usr_id in sorted(registry._name_to_id.items(),
                                    key=lambda kv: len(kv[0]), reverse=True):
        if len(name_norm) < 4:
            continue
        text = re.sub(re.escape(name_norm), usr_id, text, flags=re.IGNORECASE)

    return text


# ─── File I/O ─────────────────────────────────────────────────────────────────

def _detect_encoding(path: Path) -> str:
    """
    Try to detect file encoding. Tries chardet first (if available), then
    falls back to probing the ENCODINGS list.
    """
    try:
        import chardet
        raw = path.read_bytes()[:8192]
        result = chardet.detect(raw)
        if result["encoding"] and result["confidence"] > 0.7:
            return result["encoding"]
    except ImportError:
        pass
    # Probe by trying to read the first few lines
    for enc in ENCODINGS:
        try:
            with open(path, encoding=enc) as f:
                f.read(4096)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin-1"   # last resort — never raises on any byte


def _read_csv_robust(path: Path, encoding: str, skiprows: int) -> pd.DataFrame:
    """
    Read a CSV whose Content column may contain unquoted commas.

    The SMS Exporter format has exactly 7 fixed columns:
        Date, Time, Direction, Contact, Phone, Content, Type

    Strategy: split each line with maxsplit=(ncols-2) so everything between
    the 5th comma and the last comma becomes one field, then split that field
    from the right to peel off the final 'Type' value.

    This correctly reconstructs Content even when it contains many commas.
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
            # TSV: tabs don't appear in message content — safe to split normally
            parts = line.split("\t")
        else:
            # CSV with possible unquoted commas in Content:
            # Split into (ncols-1) parts from the left; the last part is
            # "Content_text,...,Type" — split it from the right to get Type.
            parts = line.split(",", ncols - 2)
            if len(parts) == ncols - 1:
                last_comma = parts[-1].rfind(",")
                if last_comma != -1:
                    content = parts[-1][:last_comma]
                    type_val = parts[-1][last_comma + 1:]
                    parts = parts[:-1] + [content, type_val]

        # Pad or trim to expected width
        if len(parts) < ncols:
            parts += [""] * (ncols - len(parts))
        else:
            parts = parts[:ncols]

        rows.append([p.strip().strip('"') for p in parts])

    return pd.DataFrame(rows, columns=header)


def load_file(path: Path) -> pd.DataFrame:
    """
    Load a .csv / .xlsx SMS export, robustly handling:
      - SMS Exporter 3-line metadata header (present or absent — auto-detected)
      - Tab-separated OR comma-separated values (auto-detected)
      - French column names → normalised to English equivalents
      - Unquoted commas inside the Content field (custom line-by-line reader)
      - Any encoding: UTF-8, UTF-8-BOM, Latin-1, CP-1252, etc. (auto-detected)
    """
    ext = path.suffix.lower()

    if ext == ".csv":
        encoding = _detect_encoding(path)

        # Try reading with the metadata skip first, then without
        for skip in (CSV_SKIP, 0):
            try:
                df = _read_csv_robust(path, encoding, skip)
            except pd.errors.EmptyDataError:
                continue
            if EXPECTED_COLS.intersection(df.columns):
                break
        else:
            raise ValueError(
                f"Could not find expected columns in '{path.name}'. "
                f"Columns found: {list(df.columns)}"
            )

    elif ext in (".xlsx", ".xls"):
        for skip in (CSV_SKIP, 0):
            df = pd.read_excel(path, skiprows=skip, header=0, dtype=str)
            if EXPECTED_COLS.intersection(df.columns):
                break
        else:
            raise ValueError(f"Could not find expected columns in '{path.name}'")

    else:
        raise ValueError(f"Unsupported file type: {ext}")

    # Normalise French column names to English
    df.rename(columns=COLUMN_MAP, inplace=True)

    # Ensure all cells are strings (Excel can produce floats/NaT etc.)
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

    registry   = IdentityRegistry()
    dataframes = []

    # ── Pass 1: load every file, build the global identity registry ───────────
    print("─── Pass 1: Discovering identities ───")
    for filepath in files:
        print(f"    Loading  {filepath.name} ...", end=" ", flush=True)
        try:
            df = load_file(filepath)
        except Exception as e:
            # File is unreadable — report and skip entirely (don't add to dataframes)
            print(f"ERROR — {e}")
            continue

        # File loaded successfully: always append so it gets an output in pass 2
        dataframes.append((filepath, df))

        if "Content" in df.columns:
            df["Content"].dropna().apply(lambda t: extract_identities(t, registry))
            print(f"OK  ({len(df)} rows)")
        else:
            # Still write the file in pass 2, just can't scan for identities
            print(f"WARN — 'Content' column not found after normalisation "
                  f"(columns: {list(df.columns)})")

    print(
        f"\n    Discovered  {len(registry._phone_to_id)} phone number(s)  "
        f"and  {len(registry._name_to_id)} name(s).\n"
    )

    # ── Pass 2: anonymize each loaded file and write output ───────────────────
    print("─── Pass 2: Anonymizing and saving ───")
    for filepath, df in dataframes:
        print(f"    Anonymizing  {filepath.name} ...", end=" ", flush=True)
        try:
            anon = df.copy()

            if "Content" in anon.columns:
                anon["Content"] = anon["Content"].apply(
                    lambda t: anonymize_text(t, registry)
                )

            # Phone column holds the sender label ("OrangeMoney", "MobileMoney")
            # — not a real number — leave it untouched.

            # Derive output filename: replace owner's name portion with USER_N alias.
            # SMS Exporter format: "Messages_with_OrangeMoney_DATE_-_First_Last_Name"
            # Accent-normalise the filename name so "Joël" matches the registry
            # entry built from message text which went through the same normaliser.
            stem = filepath.stem
            sep_token = " - " if " - " in stem else ("_-_" if "_-_" in stem else None)
            if sep_token:
                prefix, raw_name_part = stem.rsplit(sep_token, 1)
                raw_name   = raw_name_part.replace("_", " ").strip()
                user_alias = registry.get_name_id(raw_name)  # _norm strips accents
                label      = user_alias if user_alias else raw_name_part
                new_stem   = prefix + sep_token + label
            else:
                new_stem = stem

            # Guard against two files resolving to the same output path
            # (e.g. two people whose names aren't in the message content).
            candidate = OUTPUT_DIR / (new_stem + "_anonymized" + filepath.suffix)
            out_path  = candidate
            counter   = 1
            while out_path.exists():
                out_path = OUTPUT_DIR / (new_stem + f"_{counter}_anonymized" + filepath.suffix)
                counter += 1

            save_file(anon, out_path)
            print(f"OK  →  {out_path.name}")
        except Exception as e:
            print(f"ERROR — {e}")

    # ── Write identity map ────────────────────────────────────────────────────
    print()
    print("─── Writing identity map ───")
    id_map = registry.to_dict()
    with open(MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(id_map, f, ensure_ascii=False, indent=2)
    print(f"    Saved  →  {MAP_FILE.name}  ({len(id_map['identity_map'])} records)")
    print()
    print(f"✓ Done. Outputs are in:  {OUTPUT_DIR}")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    process_files()