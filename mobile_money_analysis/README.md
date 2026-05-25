# 📱 Mobile Money Data Extractor — v3
**CSC 3221 — Introduction to Data Science | ICT University**

---

## Overview

This notebook automates the full processing pipeline for mobile money SMS exports collected as part of the CSC 3221 data collection project. It takes raw SMS files from MTN MoMo and Orange Money, extracts only the meaningful financial transactions, anonymizes all personal identifiers, and exports clean structured datasets ready for analysis.

**No manual configuration is needed.** Just drop files in the `data/` folder and run all cells.

---

## Project Folder Structure

```
Project/
│
├── data/                          ← Put all SMS export files here
│   ├── Messages_with_OrangeMoney_..._Firstname_Lastname.csv
│   ├── Messages_with_MobileMoney_..._Firstname_Lastname.xlsx
│   └── ...                        (any number of .csv or .xlsx files)
│
├── output/                        ← Created automatically when you run the notebook
│   ├── Messages_with_OrangeMoney_..._USER_1.xlsx   (styled Excel)
│   ├── Messages_with_OrangeMoney_..._USER_1.csv    (flat CSV)
│   ├── Messages_with_MobileMoney_..._USER_2.xlsx
│   ├── Messages_with_MobileMoney_..._USER_2.csv
│   └── owner_map.json             ← Maps USER_N aliases back to real names & phones
│
├── Mobile_Money_Data_Extractor_v3.ipynb   ← Main notebook (run this)
└── README.md                              ← This file
```

---

## Getting Started

### 1. Prerequisites

You need Python 3.10+ and Jupyter. Install the required packages:

```bash
pip install pandas openpyxl chardet jupyter
```

Or if you use Anaconda / conda:

```bash
conda install pandas openpyxl jupyter
pip install chardet
```

> The notebook also installs `openpyxl` and `chardet` automatically on first run via `pip install` in cell 1, so this step is optional.

### 2. Export your SMS messages

**Android users — SMS Exporter app:**
1. Install [SMS Exporter](https://play.google.com/store/apps/details?id=com.smartpositive.sms_exporter) from the Play Store
2. Open the app → select only your **OrangeMoney** or **MobileMoney** conversation
3. Export as **CSV** → save the file

**iOS users — MsgKeep app:**
1. Install [Text Message Export – MsgKeep](https://apps.apple.com/cm/app/text-message-export-msgkeep/id6758947648) from the App Store
2. Select your mobile money conversation → export as CSV

### 3. Add your files

Copy all exported CSV/XLSX files into the `data/` folder. The notebook picks up every file automatically — no renaming needed.

### 4. Run the notebook

```bash
jupyter notebook Mobile_Money_Data_Extractor_v2.ipynb
```

Then: **Kernel → Restart & Run All**

That's it. Outputs appear in `output/` when the notebook finishes.

---

## What the Notebook Does — Step by Step

| Step | Cell | Description |
|------|------|-------------|
| 1 | Install & imports | Installs dependencies, imports all libraries |
| 2 | Folder setup | Scans `data/` and lists all files to process |
| 3 | File loading | Loads each file, auto-detects encoding, separator (tab/comma), FR/EN column names, and header offset |
| 4 | Owner detection | Identifies the file owner from message content (most-frequent phone number + paired name), with filename fallback |
| 5 | Balance filter | Keeps only messages that contain a balance change (`nouveau solde` / `new balance`) |
| 6 | Amount extraction | Extracts transaction amount, currency, and new balance value using bilingual regex patterns |
| 7 | Classification | Classifies each transaction into one of 8 types × direction (IN/OUT) |
| 8 | Anonymization | Replaces owner name/phone with `[USER_N]`/`[USER_N_phone]`; other contacts get hashed tokens `[CONTACT_XXXX]`/`[PHONE_XXXX]` |
| 9 | Excel export | Produces a styled Excel workbook with color-coded IN/OUT rows |
| 10 | Main pipeline | Runs steps 3–9 on every file in `data/` automatically |
| 11 | Summary | Prints a table of results across all processed files |
| 12 | (Optional) Inspect | Shows any messages that weren't classified — use to tune rules |
| 13 | (Optional) Owner map | Displays `owner_map.json` as a table |

---

## Output Files

### Per-user Excel (`.xlsx`)
A styled workbook with one sheet containing:

| Column | Description |
|--------|-------------|
| `UserId` | Anonymized owner alias (`USER_1`, `USER_2`, ...) |
| `Date` | Transaction date |
| `Time` | Transaction time |
| `Operator` | `OrangeMoney` or `MobileMoney` |
| `Transaction_type` | `depot`, `retrait`, `transfert`, `paiement`, `rechargement`, `airtime`, `transaction`, or `autre` |
| `Direction` | `IN` (money received) or `OUT` (money sent/spent) |
| `Amount` | Transaction amount as a number |
| `Currency` | `FCFA` or `XAF` |
| `New_balance` | Account balance after the transaction |
| `Anonymized_Content` | Full message text with all PII replaced |

Rows are color-coded: **green = IN**, **red = OUT**.

### Per-user CSV (`.csv`)
Same data as Excel in plain CSV format, UTF-8 encoded. Use this for Python/R analysis.

### `owner_map.json`
Tracks the real identity behind each alias — **keep this file private**.

```json
{
  "owner_map": [
    {
      "user_alias":    "USER_1",
      "phone_alias":   "USER_1_phone",
      "original_name": "Firstname Lastname",
      "original_phone": "6XXXXXXXXX",
      "operator":      "MobileMoney",
      "source_file":   "Messages_with_MobileMoney_..._Firstname_Lastname.csv",
      "output_xlsx":   "Messages_with_MobileMoney_..._USER_1.xlsx",
      "output_csv":    "Messages_with_MobileMoney_..._USER_1.csv",
      "transactions":  455,
      "in_count":      58,
      "out_count":     397,
      "unclassified":  0
    }
  ]
}
```

---

## Supported File Formats

| Format | Notes |
|--------|-------|
| `.csv` | Comma-separated or tab-separated, auto-detected |
| `.xlsx` / `.xls` | Excel format from SMS Exporter |
| English columns | `Date, Time, Direction, Contact, Phone, Content, Type` |
| French columns | `Date, Heure, Direction, Contact, Téléphone, Contenu, Type` |
| Encoding | UTF-8, UTF-8 BOM, Latin-1, CP1252 — all handled automatically |
| Header offset | 0 or 3 metadata rows at top — auto-detected |

---

## Anonymization Details

The notebook uses the lecturer's anonymization logic (from `Mobile_Money_Data_Extractor.ipynb`) with the following enhancements:

| Identifier | Replacement |
|-----------|------------|
| Owner's name | `[USER_N]` — consistent across all their files |
| Owner's phone (local + international) | `[USER_N_phone]` |
| Other people's names (adjacent to phone) | `[CONTACT_XXXX]` — 4-char MD5 hash, consistent per name |
| Other phone numbers | `[PHONE_XXXX]` — last 4 digits shown |
| Agent names (after `chez` / `at`) | `[CONTACT_XXXX]` |
| Remaining ALL-CAPS multi-word names | `[CONTACT_XXXX]` |

**Order of operations matters** — name+phone pairs are matched first, then phones alone, then remaining names. This prevents partial matches from breaking structured patterns.

---

## Transaction Types

| Type | Direction | Description |
|------|-----------|-------------|
| `depot` | IN | Cash deposit by an agent into your account |
| `transfert` | IN | Money received from another person |
| `transfert` | OUT | Money sent to another person |
| `retrait` | OUT | Cash withdrawal via agent |
| `paiement` | OUT | Bill payment (ENEO, CAMWATER, subscriptions, etc.) |
| `rechargement` | OUT | Phone credit top-up |
| `airtime` | OUT | Airtime purchase |
| `transaction` | OUT | Service debit (bundles, Maviance, etc.) |
| `autre` | unknown | Unmatched — inspect with Step 12 and add a rule |

---

## Troubleshooting

**`No files found in data/`**
→ Make sure your files are directly inside the `data/` folder (not in a subfolder) and have a `.csv` or `.xlsx` extension.

**`Could not find expected columns`**
→ The file may have an unusual format. Open it in Excel/LibreOffice and check that it has columns like `Date`, `Content` (or `Contenu`). The app may have exported in an unsupported format.

**Owner detected as `(unknown)`**
→ The owner's name didn't appear alongside their phone number in the messages. Rename the file to include your name separated by ` - ` or `_-_`, e.g. `MobileMoney_-_Jean_Paul_Dupont.csv` — the script will extract the name from the filename.

**Some messages unclassified (`autre`)**
→ Run Step 12 to inspect them. If you see a recurring pattern, add a new rule to `TX_RULES` in Step 7:
```python
('paiement', 'OUT', [
    r'your\s+new\s+pattern\s+here',
]),
```

**Encoding errors on French files**
→ The script tries multiple encodings automatically. If a file still fails, open it in a text editor and re-save it as UTF-8.

---

## Privacy & Ethics

- All names and phone numbers in message content are replaced before output
- `owner_map.json` is the only file linking aliases to real identities — store it securely and do not share it with the dataset
- Raw input files in `data/` should not be shared publicly
- Output files in `output/` are safe for academic use and group analysis

---

*CSC 3221 — Introduction to Data Science | Dr. Fotsing Kuetche | ICT University | Spring 2026*
