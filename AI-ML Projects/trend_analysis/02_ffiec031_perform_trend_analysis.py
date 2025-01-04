# Load packages
import os
import pandas as pd
import numpy as np
from scipy.stats import shapiro
import functools as ft
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", 100)
pd.set_option("display.width", 100)
#from scipy.stats import ks_2samp
#from scipy.stats import anderson
#from scipy.stats import zscore

# Read in datasets from SQL DBs
"""Placeholder"""


# Read in datasets from a local driver
rootdir = Path.cwd()
file_path_processed = rootdir / "inputs" / "FFIEC_031_Processed.xlsx"
file_path_reference = rootdir / "inputs" / "FFIEC_031_Reference.xlsx"
df_processed = pd.read_excel(file_path_processed)
df_reference = pd.read_excel(file_path_reference)


# Perform zero/non-zero balance analysis
def perform_zero_balance_analysis(proc_data: pd.DataFrame, ref_data: pd.DataFrame) -> pd.DataFrame:
    """
       Analyze zero balance breaches in financial reporting data based on specific reporting frequency rules.
       Assumes a number of all quarter-end reporting line items are identical
       Parameters:
           proc_data : Processed DataFrame containing financial data to be analyzed.
           ref_data : Reference DataFrame with additional metadata about reporting frequency.
       Returns: A DataFrame with the same rows as the current reporting period in `proc_data`
    """
    current_report_date = proc_data["ReportDate"].max()
    proc_data_current = proc_data[proc_data["ReportDate"] == current_report_date]
    proc_data_current_freq = proc_data_current.merge(ref_data[["MDRM", "ReportingFrequency"]], on="MDRM", how="left")

    def check_zero_balance_breached(row):
        freq = row["ReportingFrequency"]
        value = row["Value"]
        month = row["ReportDate"].month

        if freq == "Quarterly with Zeros":
            return "Yes" if value != 0 or pd.isna(value) else "No"

        if freq == "Quarterly - Not Reported":
            return "Yes" if not pd.isna(value) else "No"

        if freq == "Semi-Annually with Zeros":
            if month in [6, 12]:
                return "Yes" if value != 0 or pd.isna(value) else "No"
            return "Yes" if not pd.isna(value) else "No"

        if freq == "Annually with Zeros":
            if month == 12:
                return "Yes" if value != 0 or pd.isna(value) else "No"
            return "Yes" if not pd.isna(value) else "No"

        if freq == "Quarterly with Non-zeros":
            return "Yes" if value == 0 or pd.isna(value) else "No"

        if freq == "Semi-Annually with Non-zeros":
            if month in [6, 12]:
                return "Yes" if value == 0 or pd.isna(value) else "No"
            return "Yes" if not pd.isna(value) else "No"

        if freq == "Annually with Non-zeros":
            if month == 12:
                return "Yes" if value == 0 or pd.isna(value) else "No"
            return "Yes" if not pd.isna(value) else "No"

        return "No"

    proc_data_current_freq["ZeroBalance_Breached"] = proc_data_current_freq.apply(check_zero_balance_breached, axis=1)
    proc_data_current_freq = proc_data_current_freq.drop("ReportingFrequency", axis=1)

    return proc_data_current_freq[["MDRM", "ZeroBalance_Breached"]]


# Perform variance analysis
def perform_variance_analysis(proc_data: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze the variance between current and prior reporting period values.
    Args: proc_data (pd.DataFrame): A DataFrame containing historical processed data
    Returns: pd.DataFrame: A DataFrame combining the data from the current and prior reporting periods.
    """
    # Extract and sort unique report dates in descending order
    unique_dates = sorted(proc_data["ReportDate"].unique(), reverse=True)
    current_report_date, prior_report_date = unique_dates[:2]

    # Split data into current and prior periods
    proc_data_current = proc_data[proc_data["ReportDate"] == current_report_date].rename(columns={"Value": "Value_Curr"})
    proc_data_prior = proc_data[proc_data["ReportDate"] == prior_report_date][["MDRM", "Value"]].rename(columns={"Value": "Value_Prev"})
    proc_data_combined = proc_data_current.merge(proc_data_prior, on="MDRM", how="left")

    # Calculate absolute variance
    proc_data_combined["Variance_Amt"] = proc_data_combined["Value_Curr"] - proc_data_combined["Value_Prev"]

    # Initialize variance percentage
    proc_data_combined["Variance_Pct"] = 0

    # Calculate percentage variance where previous value is non-zero
    non_zero_prev = proc_data_combined["Value_Prev"] != 0
    proc_data_combined.loc[non_zero_prev, "Variance_Pct"] = (
        (proc_data_combined["Variance_Amt"] / proc_data_combined["Value_Prev"].abs()) * 100
    ).round(2)

    # Handle cases where the previous value is zero but the current value is non-zero
    zero_prev_non_zero_curr = (proc_data_combined["Value_Prev"] == 0) & (proc_data_combined["Value_Curr"] != 0)
    proc_data_combined.loc[zero_prev_non_zero_curr, "Variance_Pct"] = 100

    return proc_data_combined


# Function to perform Shapiro-Wilk normality test per MDRM
def perform_shapiro_wilk_test(proc_data: pd.DataFrame) -> pd.DataFrame:

    current_report_date = proc_data["ReportDate"].max()
    proc_data_prior = proc_data[proc_data["ReportDate"] != current_report_date]
    unique_mdrms = proc_data_prior['MDRM'].unique()

    sw_test = []
    sw_test.append(["MDRM", "SW_pval", "Normality"])
    for mdrm in unique_mdrms:
        mdrm_value = proc_data_prior.loc[proc_data_prior["MDRM"] == mdrm, "Value"].dropna()
        if len(mdrm_value) >= 3:
            mdrm_sw_pvalue = round(shapiro(mdrm_value).pvalue, 4)
            normality = "No" if mdrm_sw_pvalue < 0.05 else "Yes"
        else:
            mdrm_sw_pvalue = float("NaN")
            normality = "Insufficient Data"
        sw_test.append([mdrm, mdrm_sw_pvalue, normality])
    sw_test_result = pd.DataFrame(sw_test[1:], columns=sw_test[0])

    return sw_test_result


# Function to calculate standard or robust z-scores to detect outliers
def perform_outlier_detection(proc_data: pd.DataFrame, ref_data: pd.DataFrame) -> pd.DataFrame:
    # Helper Functions
    def categorize_mdrms(ref_data):
        """Categorize MDRMs based on their reporting frequency."""
        categories = {"quarterly": [], "semi_annual": [], "annual": [], "non_reporting": []}
        for idx, row in ref_data.iterrows():
            if row["ReportingFrequency"].startswith("Quarterly with"):
                categories["quarterly"].append(row["MDRM"])
            elif row["ReportingFrequency"].startswith("Semi-Annually"):
                categories["semi_annual"].append(row["MDRM"])
            elif row["ReportingFrequency"].startswith("Annually"):
                categories["annual"].append(row["MDRM"])
            else:
                categories["non_reporting"].append(row["MDRM"])
        return categories

    def extract_recent_periods(proc_data, num_quarters=12):
        """Extract relevant report dates for quarterly, semi-annual, and annual categories."""
        unique_dates = sorted(proc_data["ReportDate"].unique(), reverse=True)
        quarter_dates = unique_dates[1:num_quarters + 1]
        semi_annual_dates = [d for d in proc_data["ReportDate"] if d.month in [6, 12]][-10:]
        annual_dates = [d for d in proc_data["ReportDate"] if d.month == 12][-10:]
        return quarter_dates, semi_annual_dates, annual_dates

    def identify_outliers(proc_data_prior, proc_data_current, sw_result, normal_mdrms, notnormal_mdrms):
        """Calculate z-scores for normal and non-normal MDRMs."""
        # Process normal MDRMs
        normal = proc_data_prior[proc_data_prior["MDRM"].isin(normal_mdrms)].dropna()
        normal_avgstd = (normal.groupby("MDRM")["Value"]
                         .agg(["mean", "std"])
                         .reset_index()
                         .rename(columns={"mean": "Value_mean", "std": "Value_std"}))

        # Process non-normal MDRMs
        not_normal = proc_data_prior[proc_data_prior["MDRM"].isin(notnormal_mdrms)].dropna()
        not_normal["Value_absdev"] = abs(not_normal["Value"] - not_normal["Value"].median())
        not_normal_median_mad = (not_normal.groupby("MDRM")
                                 .agg(Value_median=("Value", "median"),
                                      Value_mad=("Value_absdev", "median"))
                                 .reset_index())

        # Merge all components
        zscore_data = ft.reduce(
            lambda left, right: pd.merge(left, right, on="MDRM", how="left"),
            [proc_data_current,
             sw_result[["MDRM", "Normality"]],
             normal_avgstd,
             not_normal_median_mad]
        )

        # Calculate Outlier Scores
        def calculate_zscores(row):
            if row["Normality"] == "Yes":
                return (row["Value"] - row["Value_mean"]) / row["Value_std"] if row["Value_std"] > 0 else 0
            elif row["Normality"] == "No":
                return 0.6745 * (row["Value"] - row["Value_median"]) / row["Value_mad"] if row["Value_mad"] > 0 else 0
            return 0

        zscore_data["OutlierScore"] = zscore_data.apply(calculate_zscores, axis=1)
        zscore_data["Outlier"] = np.where(zscore_data["OutlierScore"].abs() > 3, "Yes", "No")

        return zscore_data

    # Main Workflow
    sw_result = perform_shapiro_wilk_test(proc_data)
    proc_data_current = proc_data[proc_data["ReportDate"] == proc_data["ReportDate"].max()]
    proc_data_prior = proc_data[proc_data["ReportDate"] != proc_data["ReportDate"].max()]

    # Process normality categories
    normal_mdrms = sw_result.loc[sw_result["Normality"] == "Yes", "MDRM"]
    notnormal_mdrms = sw_result.loc[sw_result["Normality"] == "No", "MDRM"]

    # Categorize MDRMs
    mdrm_categories = categorize_mdrms(ref_data)
    quarter_dates, semi_annual_dates, annual_dates = extract_recent_periods(proc_data)

    # Filter prior data by frequency
    prior_quarter = proc_data_prior[proc_data_prior["MDRM"].isin(mdrm_categories["quarterly"])]
    prior_12qtrs = prior_quarter[prior_quarter["ReportDate"].isin(quarter_dates)]

    prior_semi_annual = proc_data_prior[proc_data_prior["MDRM"].isin(mdrm_categories["semi_annual"])]
    prior_semi_annual_5yrs = prior_semi_annual[prior_semi_annual["ReportDate"].isin(semi_annual_dates)]

    prior_annual = proc_data_prior[proc_data_prior["MDRM"].isin(mdrm_categories["annual"])]
    prior_annual_10yrs = prior_annual[prior_annual["ReportDate"].isin(annual_dates)]

    prior_non_reporting = proc_data_prior[proc_data_prior["MDRM"].isin(mdrm_categories["non_reporting"])]

    # Calculate z-scores for each category
    prior_data_categories = [prior_12qtrs, prior_semi_annual_5yrs, prior_annual_10yrs, prior_non_reporting]
    prior_data_zscores = []
    for prior_data_category in prior_data_categories:
        relevant_current = proc_data_current[proc_data_current["MDRM"].isin(prior_data_category["MDRM"].unique())]
        zscore_results = identify_outliers(prior_data_category, relevant_current, sw_result, normal_mdrms, notnormal_mdrms)
        prior_data_zscores.append(zscore_results)

    prior_data_outliers = pd.concat(prior_data_zscores, axis=0, ignore_index=True)
    return prior_data_outliers[["MDRM", "OutlierScore", "Outlier"]]


# Create field instruction
def create_field_instruction():
    fld_inst = [["Section", "Field", "Field Description", "Field Value Example"]]
    fld_inst.append(["Analysis Detail", "ReportDate", "The year and date when the Call Report is filed.", "20240331"])
    fld_inst.append(["Analysis Detail", "MDRM", "Micro Data Reference Manual: a catalog of data series in the Board's microdata files.", "RCFA3792"])
    fld_inst.append(["Analysis Detail", "Value_Curr", "The current quarter's reported balance of a line item, in USD thousands.", "1000000"])
    fld_inst.append(["Analysis Detail", "Value_Prev", "The previous quarter's reported balance of a line item, in USD thousands.", "1000000"])
    fld_inst.append(["Analysis Detail", "Variance_Amt", "The difference in reported balances between the current and previous quarters, in USD thousands.", "1000000"])
    fld_inst.append(["Analysis Detail", "Variance_Pct", "The percentage difference in reported balances between the current and previous quarters.", "0.67"])
    fld_inst.append(["Analysis Detail", "OutlierScore", "The standard or robust Z-score of the current quarter's reported balance of a line item.", "2.67"])
    fld_inst.append(["Analysis Detail", "Outlier", "Indicates whether the current quarter's reported balance is an outlier based on the absolute OutlierScore exceeding 3.", "Yes"])
    fld_inst.append(["Analysis Detail", "ZeroBalance_Breached", "Indicates whether the balance is zero or non-zero, flagged as Yes if it deviates from historical reporting patterns.", "Yes"])
    fld_inst.append(["Analysis Detail", "Schedule", "The reporting schedule associated with the line item.", "RCRIB"])
    fld_inst.append(["Analysis Detail", "LineNumber", "The line number assigned to the reporting item.", "35a"])
    fld_inst.append(["Analysis Detail", "Definition", "A brief definition of the line item.", "Total capital (sum of items 26 and 34.a)"])
    fld_inst.append(["Analysis Detail", "ReportingFrequency", "The frequency of reporting the line item: quarterly, semi-annually, or annually.", "Quarterly with Non-zeros"])

    fld_inst_df = pd.DataFrame(fld_inst[1:], columns=fld_inst[0])
    fld_inst_df_style_setup = fld_inst_df.style.set_table_styles(
        {
            "Section": [{"selector":"", "props":[("text_align", "center")]}],
            "Field": [{"selector":"", "props":[("text_align", "center")]}],
            "Field Description": [{"selector":"", "props":[("text_align", "left")]}],
            "Field Value Example": [{"selector":"", "props":[("text_align", "left")]}]
         }
    )
    return fld_inst_df_style_setup


def create_excel_from_list(data, excel_file):
    sheet_names = ["Filed Instruction", "Analysis Detail", "Historical Data"]
    with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
        data[0].to_excel(writer, sheet_name=sheet_names[0], index=False)
        data[1].to_excel(writer, sheet_name=sheet_names[1], index=False)
        data[2].to_excel(writer, sheet_name=sheet_names[2], index=False)


def create_output_folder():
    output_folder_path = os.getcwd() + "\\outputs\\"
    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)
    return output_folder_path


def perform_ffiec031_trend_analysis(proc_data, ref_data):
    today_date = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    output_folder = create_output_folder()
    excel_file_path = output_folder + f"FFIEC031_TrendAnalysis_Result_{today_date}.xlsx"

    zero_balance_analysis = perform_zero_balance_analysis(proc_data, ref_data)
    variance_analysis = perform_variance_analysis(proc_data)
    outlier_detection = perform_outlier_detection(proc_data, ref_data)
    analysis_detail = ft.reduce(
        lambda left, right: pd.merge(left, right, on="MDRM", how="left"),
        [variance_analysis,
                  outlier_detection,
                  zero_balance_analysis,
                  ref_data]
                )

    field_instruction = create_field_instruction()
    final_data = [field_instruction, analysis_detail, proc_data]
    create_excel_from_list(final_data, excel_file_path)

    return analysis_detail


if __name__ == "__main__":
    perform_ffiec031_trend_analysis(df_processed, df_reference)


