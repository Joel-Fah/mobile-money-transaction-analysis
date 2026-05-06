# Data Cleaning Report: Mobile Money Transaction Analysis

## Introduction

This report documents the data cleaning process performed on the mobile money transaction dataset. The goal was to transform the initial raw data into a structured, reliable, and analysis-ready format, ensuring accuracy and consistency for downstream analysis.

## 1. Raw Data Overview

The initial dataset (`raw_data_anonymized.csv`) contained transaction records with the following columns:
- UserId
- Date, Time
- Operator
- Transaction_type
- Direction
- Amount, Currency
- New_balance
- Anonymized_Content (full SMS message text)

**Issues identified in the raw data:**
- Inconsistent date/time formats
- Mixed currencies (XAF, FCFA)
- Duplicated or near-duplicate records
- Non-standardized transaction types and directions
- Embedded information in free-text fields
- Missing or anomalous values

## 2. Data Cleaning Steps

The following steps were applied to clean and standardize the data:

### a. Standardization
- Unified date and time formats to ISO standard.
- Standardized currency representation (converted all to XAF where needed).
- Normalized transaction types and directions (e.g., 'paiement', 'transfert', 'retrait', 'transaction'; 'IN', 'OUT').

### b. Deduplication
- Removed exact and near-duplicate records based on UserId, Date, Time, Amount, and Transaction_type.

### c. Handling Missing and Anomalous Values
- Imputed or removed records with missing critical fields (e.g., Amount, Date).
- Flagged and reviewed outliers in transaction amounts and balances.

### d. Feature Extraction
- Parsed and extracted structured information from the `Anonymized_Content` field (e.g., sender/receiver, transaction IDs).
- Calculated derived features such as transaction frequency, average/median amounts, and balance statistics.

### e. Anonymization
- Ensured all personal identifiers were replaced with anonymized tokens (e.g., [CONTACT_xxxx], [PHONE_xxxx]).

## 3. Cleaned Data Overview

The cleaned dataset (`cleaned_data.csv`) contains aggregated and feature-engineered records per user, with columns such as:
- UserId
- Total transactions, months active, tx per month
- Average, median, std, and total amounts (in/out)
- Transaction type counts and ratios (send/receive, weekend ratio)
- Balance statistics (average, min, max)
- Recency, velocity, and activity labels
- Demographic and socioeconomic features (age, gender, occupation, education, income, etc.)

**Example (USER_1):**
| UserId  | total_transactions | months_active | avg_amount | total_amount_in | total_amount_out | send_receive_ratio | avg_balance | activity_label |
|---------|-------------------|---------------|------------|-----------------|------------------|-------------------|-------------|---------------|
| USER_1  | 147               | 30            | 9872.82    | 779704.0        | 671600.0         | 2.267             | 14101.95    | Low           |

## 4. Key Changes and Improvements
- Reduced noise and inconsistencies in transaction records.
- Enhanced data quality by removing duplicates and correcting errors.
- Added valuable features for analysis (e.g., transaction ratios, demographic links).
- Ensured privacy through robust anonymization.

## 5. Conclusion

The data cleaning process has resulted in a high-quality, anonymized, and analysis-ready dataset. This cleaned data forms the foundation for robust statistical analysis and modeling of mobile money usage patterns.

---
*For further details, refer to the code and scripts used in the data cleaning pipeline.*
