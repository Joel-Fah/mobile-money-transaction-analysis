# Final Assessment: Complete Data Science Project - Mobile Money Transaction Analysis 

- **Assessment Type**: Group Project, Report & Presentation 
- **Group Size**: 5 students per group 
- **Submission Deadline**: Sunday, 3rd May, 2026 11:59 PM WAT 
- **Submission Format**: Submit a .zip file (see "Submission Instructions" below)

## Context 

Mobile money services (MTN Mobile Money, Orange Money) have transformed financial transactions in Cameroon and across Africa. Understanding transaction patterns can help: 
- Financial institutions assess credit risk 
- Service providers improve user experience 
- Businesses optimize payment solutions 
- Regulators monitor financial inclusion 

## Your Task 

Analyze mobile money transaction patterns to predict user behavior or classify user segments based on transaction history and demographic characteristics.

**Possible Prediction Targets** (choose ONE): 
1. Transaction Volume Prediction: Predict next month's transaction volume/amount 
2. User Classification: Classify users into behavioral segments (e.g., high/medium/low activity) 
3. Credit Risk Assessment: Estimate creditworthiness based on transaction patterns 
4. Custom Problem: Propose and justify your own prediction target (requires instructor approval) 

## Learning Outcomes

By completing this assessment, you will demonstrate the ability to: 
- Collect real-world data ethically and systematically, ensuring data quality and privacy 
- Clean and wrangle messy, real-world data using appropriate techniques 
- Explore data using descriptive statistics and effective visualizations 
- Apply statistical methods and machine learning algorithms to make predictions 
- Evaluate model performance using appropriate metrics and validation techniques 
- Communicate data science findings clearly to both technical and non-technical audiences 
- Collaborate effectively in a team environment, managing a complex analytical project 
- Practice ethical data science, including privacy protection and bias awareness 

## Data Collection & Privacy Tools

This repository includes custom scripts to help you automate data collection consent and securely anonymize sensitive mobile money datasets.

### 1. Generating Consent Forms (`consent_code.gs`)
This Google Apps Script automates the generation of PDF consent forms by merging participant data from a Google Sheet, a Google Docs template, and signature images.

**How to use:**
1. Open your Google Sheet containing the participant consent tracker.
2. Go to **Extensions > Apps Script** and paste the contents of `consent_code.gs`.
3. Update the `CONFIG` object at the top of the script with your specific Google Drive Folder IDs and Template Document ID (`TEMPLATE_ID`, `PARENT_FOLDER_ID`, etc.).
4. Save the script.
5. You can execute `generateConsents` directly from the Apps Script editor (ignore any UI warnings), or reload your Google Sheet and use the custom **🚀 Research Tools > Generate Consent PDFs** menu.
6. Auto-generated PDFs will be dumped into the destination folder, and the sheet's status column will read "Completed".

### 2. Standard Anonymization (`anonymize.py`)
This script processes your raw SMS export files (CSV or Excel) to replace all personally identifiable information (names, phone numbers) with consistent aliases (e.g., `USER_1`, `PH_NUMBER_1`) while preserving the relational dynamics of the data.

**How to use:**
1. Place your raw SMS export `.csv` or `.xlsx` files inside a folder named `data/` located in the same directory as the script.
2. Install the necessary Python dependencies: 
   ```bash
   pip install pandas openpyxl chardet
   ```
3. Run the script: 
   ```bash
   python anonymize.py
   ```
4. The anonymized data files will be created in an `output/` directory, along with a mapping file (`identity_map.json`) to keep track of the original names/numbers mapped to their new aliases.

### 3. Strict Owner-Only Anonymization (`anonymize_xxx.py`)
Use this variant if you need maximum privacy for third-party contacts. It provides the file owner with a consistent alias, but thoroughly redacts all other third-party names and phone numbers in the SMS contents with fixed masks (`XXXXXXXXX` / `XXXXXXXXXXX`).

**How to use:**
1. Place your raw `.csv` or `.xlsx` files within the `data/` folder.
2. Ensure you have the same Python dependencies installed (`pandas openpyxl chardet`).
3. Run the script: 
   ```bash
   python anonymize_xxx.py
   ```
4. The output files are saved securely into the `output_xxx/` directory next to an `owner_map.json` which tracks the identities of the primary data owners. 
