# Load packages
import pandas as pd
from xml.etree import ElementTree as ET

# Appendix
def extract_data_from_xml(xml_file):
    """
    Parses the XML file and extracts relevant tag and attribute data.
    Returns a dictionary of parsed data.
    """
    parser = ET.XMLParser(recover=True)
    tree = ET.parse(xml_file, parser)
    root = tree.getroot()

    tag_data = {}
    for child in root:
        if all(key in child.attrib for key in ["unitRef", "decimals"]):
            tag_data[child.tag[-8:]] = {
                "Value": child.text,
                "UnitRef": child.attrib["unitRef"],
                "Decimals": child.attrib["decimals"],
            }
    return tag_data


def process_sub_file_inner(xml_file, hist_data):
    """
    Processes a single XML submission file and filters relevant data based on historical MDRMs.
    """
    tag_data = extract_data_from_xml(xml_file)
    sub_df = pd.DataFrame.from_dict(tag_data, orient="index").reset_index()
    sub_df = sub_df.rename(columns={"index": "MDRM", "Value": "Value", "UnitRef": "UnitRef", "Decimals": "Decimals"})

    # Filter by historical MDRMs
    valid_mdrms = hist_data["MDRM"].unique()
    sub_df = sub_df[sub_df["MDRM"].isin(valid_mdrms)]

    # Convert Value to float
    sub_df["Value"] = pd.to_numeric(sub_df["Value"], errors="coerce")

    # Adjust Values based on UnitRef
    unit_adjustments = {
        "USD": lambda x: x / 1000,
        "PURE": lambda x: x * 100
    }
    sub_df["Value"] = sub_df.apply(
        lambda row: unit_adjustments.get(row["UnitRef"], lambda x: x)(row["Value"]), axis=1
    )

    return sub_df.drop(columns=["UnitRef", "Decimals"])


def get_report_dates_from_filename(xml_file_name):
    """
    Extracts report dates based on XML file naming conventions.
    """
    if xml_file_name.startswith("RD FFIEC_031_0000480228_"):
        grr_rpt_dt = xml_file_name[-14:-4].replace('-', '')
        return grr_rpt_dt, None
    elif xml_file_name.startswith("FFEIC_031_PROD_"):
        ax_rpt_dt = xml_file_name.split('_')[-3]
        return None, ax_rpt_dt
    else:
        raise ValueError("FFIEC 031 XML submission file's naming convention has changed.")


def process_sub_file_outer(xml_file, hist_data):
    """
    Processes an outer XML file, adding extracted ReportDate and preparing clean submission data.
    """
    if not xml_file:
        raise ValueError("FFIEC 031 XML submission file cannot be found.")

    xml_file_name = xml_file.split("\\")[-1]
    grr_rpt_dt, ax_rpt_dt = get_report_dates_from_filename(xml_file_name)

    clean_sub_df = process_sub_file_inner(xml_file, hist_data)
    clean_sub_df["ReportDate"] = grr_rpt_dt or ax_rpt_dt
    return clean_sub_df[["ReportDate", "MDRM", "Value"]]


def combine_hist_sub_data(xml_file, hist_data):
    """
    Combines historical data with the processed XML submission data.
    """
    sub_df = process_sub_file_outer(xml_file, hist_data)
    combined_df = (
        pd.concat([hist_data, sub_df], axis=0)
        .sort_values(by=["MDRM", "ReportDate"], ascending=[True, True])
        .reset_index(drop=True)
    )
    return combined_df


for idx, row in df_current.iterrows():
    for mdrm in zero_quarter_mdrms:
       if row['MDRM'] == mdrm:
           if row['Value'] != 0 or pd.isna(row['Value']):
               df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
           else:
               df_current.at[idx, 'ZeroBalance_Breached'] = 'No'

    for mdrm in zero_quarter_novalue_mdrms:
        if row['MDRM'] == mdrm:
            if not pd.isna(row['Value']):
                df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
            else:
                df_current.at[idx, 'ZeroBalance_Breached'] = 'No'

    for mdrm in zero_semi_annual_mdrms:
        if row['MDRM'] == mdrm:
            if row['ReportDate'].month in [6, 12]:
                if row['Value'] != 0 or pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'
            else:
                if not pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'

    for mdrm in zero_annual_mdrms:
        if row['MDRM'] == mdrm:
            if row['ReportDate'].month == 12:
                if row['Value'] != 0 or pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'
            else:
                if not pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'

    for mdrm in nonzero_quarter_mdrms:
        if row['MDRM'] == mdrm:
            if row['Value'] == 0 or pd.isna(row['Value']):
                df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
            else:
                df_current.at[idx, 'ZeroBalance_Breached'] = 'No'

    for mdrm in nonzero_semi_annual_mdrms:
        if row['MDRM'] == mdrm:
            if row['ReportDate'].month in [6, 12]:
                if row['Value'] == 0 or pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'
            else:
                if not pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'

    for mdrm in nonzero_annual_mdrms:
        if row['MDRM'] == mdrm:
            if row['ReportDate'].month == 12:
                if row['Value'] == 0 or pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'
            else:
                if not pd.isna(row['Value']):
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'Yes'
                else:
                    df_current.at[idx, 'ZeroBalance_Breached'] = 'No'


#def perform_zero_balance_analysis(proc_data: pd.DataFrame) -> pd.DataFrame:

unique_dates = sorted(df['ReportDate'].unique(), reverse=True)
current_report_date = unique_dates[0]
recent_8quarters = unique_dates[1:9]
semi_annual_dates = [date for date in recent_8quarters if date.month in [6, 12]]
annual_dates = [date for date in recent_8quarters if date.month == 12]

# Filter current and historical data
df_current = df[df['ReportDate'] == current_report_date]
df_wo_current = df[df['ReportDate'] != current_report_date]

# Initialize MDRM lists
zero_quarter_mdrms = []
zero_quarter_novalue_mdrms = []
zero_semi_annual_mdrms = []
zero_annual_mdrms = []

nonzero_annual_mdrms = []
nonzero_semi_annual_mdrms = []
nonzero_quarter_mdrms = []

proc_mdrms = df_wo_current['MDRM'].unique()


for mdrm in proc_mdrms:

    # Filter data for the current MDRM
    mdrm_data = df_wo_current[df_wo_current['MDRM'] == mdrm]
    total_value = mdrm_data['Value'].sum(skipna=True)

    # Get recent 8-quarter data
    recent_8quarter_data = mdrm_data[mdrm_data['ReportDate'].isin(recent_8quarters)]
    recent_8quarter_value = recent_8quarter_data['Value'].sum(skipna=True)

    # Classify MDRMs based on their total and recent 8-quarter values
    if total_value == 0 or recent_8quarter_value == 0:
        #zero_mdrms.append(mdrm)

        # Additional processing for zero MDRMs
        zero_values = recent_8quarter_data['Value']
        semi_annual_data = recent_8quarter_data[recent_8quarter_data['ReportDate'].isin(semi_annual_dates)].dropna()
        annual_data = recent_8quarter_data[recent_8quarter_data['ReportDate'].isin(annual_dates)].dropna()

        if zero_values.isna().any():
            missing_count = zero_values.isna().sum()

            if missing_count == 6 and len(annual_data.loc[annual_data['Value'] == 0]) == 2:
                zero_annual_mdrms.append(mdrm)
            elif missing_count == 4 and len(semi_annual_data.loc[semi_annual_data['Value'] == 0]) == 4:
                zero_semi_annual_mdrms.append(mdrm)
            else:
                zero_quarter_novalue_mdrms.append(mdrm)
        else:
            zero_quarter_mdrms.append(mdrm)

    else:
        non_zero_values = recent_8quarter_data['Value'].dropna()
        if len(non_zero_values) == 8 and non_zero_values.iloc[-1] == 0:
            zero_quarter_mdrms.append(mdrm)
        else:
            if len(non_zero_values) == 2:
                nonzero_annual_mdrms.append(mdrm)
            elif len(non_zero_values) == 4:
                nonzero_semi_annual_mdrms.append(mdrm)
            else:
                nonzero_quarter_mdrms.append(mdrm)


# Categorize MDRMs
mdrm_categories = {
    "zero_quarter": zero_quarter_mdrms,
    "zero_quarter_novalue": zero_quarter_novalue_mdrms,
    "zero_semi_annual": zero_semi_annual_mdrms,
    "zero_annual": zero_annual_mdrms,
    "nonzero_quarter": nonzero_quarter_mdrms,
    "nonzero_semi_annual": nonzero_semi_annual_mdrms,
    "nonzero_annual": nonzero_annual_mdrms,
}

df_current2 = df_current.copy()

# Function to determine "ZeroBalance_Breached" status
def check_zero_balance_breached(row, category):
    if category == "zero_quarter":
        return "Yes" if row["Value"] != 0 or pd.isna(row["Value"]) else "No"

    if category == "zero_quarter_novalue":
        return "Yes" if not pd.isna(row["Value"]) else "No"

    if category == "zero_semi_annual":
        if row["ReportDate"].month in [6, 12]:
            return "Yes" if row["Value"] != 0 or pd.isna(row["Value"]) else "No"
        return "Yes" if not pd.isna(row["Value"]) else "No"

    if category == "zero_annual":
        if row["ReportDate"].month == 12:
            return "Yes" if row["Value"] != 0 or pd.isna(row["Value"]) else "No"
        return "Yes" if not pd.isna(row["Value"]) else "No"

    if category == "nonzero_quarter":
        return "Yes" if row["Value"] == 0 or pd.isna(row["Value"]) else "No"

    if category == "nonzero_semi_annual":
        if row["ReportDate"].month in [6, 12]:
            return "Yes" if row["Value"] == 0 or pd.isna(row["Value"]) else "No"
        return "Yes" if not pd.isna(row["Value"]) else "No"

    if category == "nonzero_annual":
        if row["ReportDate"].month == 12:
            return "Yes" if row["Value"] == 0 or pd.isna(row["Value"]) else "No"
        return "Yes" if not pd.isna(row["Value"]) else "No"

    return "No"  # Default case for safety

# Apply categorization
for category, mdrms in mdrm_categories.items():
    df_current2.loc[df_current2["MDRM"].isin(mdrms), "ZeroBalance_Breached"] = df_current2[df_current2["MDRM"].isin(mdrms)].apply(
        lambda row: check_zero_balance_breached(row, category), axis=1
    )


# Option 1 - ODBC
def read_from_sqlserver_odbc(query: str) -> pd.DataFrame:
    conn_str = (
        r'Driver=ODBC Driver 17 for SQL Server;'  # ODBC SQL Server drive installed in a local drive
        r'Server=host.name.company.com,15001;'    # host name
        r'Database=DBNAME'                        # DB name
        r'Trusted_Connection=yes;'
    )
    cnxn = pyodbc.connect(conn_str)
    sqlsever_df = pd.read_sql_query(query, cnxn)
    return sqlsever_df



# Option 2 - SQLAlchemy
def read_from_sqlserver_alchemy():
    # Connection string for SQLAlchemy
    engine_str = (
        'mssql+pyodbc://host.name.company.com:15001/DBNAME'
        '?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    )

    engine = create_engine(engine_str)
    sqlsever_df = pd.read_sql_query(query, engine)
    return sqlsever_df
