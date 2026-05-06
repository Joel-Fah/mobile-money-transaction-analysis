# Final Assessment: Complete Data Science Project - Mobile Money Transaction Analysis

## Group Members

1. DEJON FAH JOEL XAVIER - ICTU20241127
2. ACHA BLESSING - ICTU20234240
3. KUMIGHEM BRYAN KENAH - ICTU20234149

## Project Title

Mobile Money Transaction Analysis

## File Structure Explanation

The project is organized into the following directories corresponding to the data science pipeline:

*   **1_Data_Collection/**: Contains the initial collected and anonymized dataset (`raw_data_anonymized.csv`).
*   **2_Data_Cleaning/**: Contains the data cleaning Jupyter Notebook (`data_cleaning.ipynb`), the resulting clean dataset (`cleaned_data.csv`), and the data cleaning report (`data_cleaning_report.md`).
*   **3_EDA/**: Contains the exploratory data analysis notebook (`exploratory_analysis.ipynb`) and generated visualization assets in the `visualizations/` folder.
*   **4_Modeling/**: Contains the machine learning scripts and notebooks (`modeling.ipynb`, `run_modeling.py`) and the `results/` directory which saves outputs like classification reports, feature importance, and model comparisons.
*   **5_Report/**: Contains final project reports and presentation materials.

## How to run the code

1.  **Data Cleaning**: Navigate to `2_Data_Cleaning/` and run `data_cleaning.ipynb` to process the raw data into `cleaned_data.csv`.
2.  **Exploratory Data Analysis**: Navigate to `3_EDA/` and run `exploratory_analysis.ipynb` to generate visual insights and summary statistics.
3.  **Modeling**: 
    *   You can run the interactive notebook `4_Modeling/modeling.ipynb`.
    *   Alternatively, execute the Python script from your terminal: `python 4_Modeling/run_modeling.py`. This will train the models and output the performance metrics in the `4_Modeling/results/` directory.

## Dependencies / requirements

To run the notebooks and scripts, ensure you have the following installed:
*   Python 3.8+
*   pandas
*   numpy
*   matplotlib
*   seaborn
*   scikit-learn
*   jupyter

You can install the required packages using pip:
```bash
pip install pandas numpy matplotlib seaborn scikit-learn jupyter
```