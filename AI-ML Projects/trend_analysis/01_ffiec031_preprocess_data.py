# Load packages
import os
import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 100)


# Read in datasets from a SQL database: Deactivated - Modify code quoted below
def read_from_sqlserver(query: str, method: str = "odbc") -> pd.DataFrame:
    """
    Reads data from a SQL Server using either ODBC or SQLAlchemy.
    Args:
        - query (str): The SQL query string.
        - method (str): The connection method to use ('odbc' or 'alchemy'). Defaults to 'odbc'.
    Returns: pd.DataFrame: The resulting dataset as a DataFrame.
    Raises:
        ValueError: If both connection methods fail or an invalid method is provided.
    """
    if method not in ["odbc", "alchemy"]:
        raise ValueError("Invalid method. Use 'odbc' or 'alchemy'.")

    if method == "odbc":
        try:
            conn_str = (
                r'Driver=ODBC Driver 17 for SQL Server;'  # ODBC SQL Server drive installed in a local drive
                r'Server=host.name.company.com,15001;'  # host name
                r'Database=DBNAME'  # DB name
                r'Trusted_Connection=yes;'
            )
            cnxn = pyodbc.connect(conn_str)
            sqlserver_df = pd.read_sql_query(query, cnxn)
            cnxn.close()
        except Exception as e:
            print(f"ODBC connection failed: {e}. Trying SQLAlchemy...")

    if method == "alchemy":
        try:
            engine_str = (
                'mssql+pyodbc://host.name.company.com:15001/DBNAME'
                '?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
            )
            engine = create_engine(engine_str)
            sqlserver_df = pd.read_sql_query(query, engine)
            engine.dispose()
            return sqlserver_df
        except Exception as e:
            print(f"SQLAlchemy connection failed: {e}")

    raise ValueError("Both ODBC and SQLAlchemy connection failed. Check your configuration.")


# Read in datasets from a local drive
def load_ffiec031_data(rootdir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads historical and static FFIEC data from specified directories.
    Args: rootdir (Path): The root directory containing the input files.
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - hist_df (pd.DataFrame): Concatenated historical data from .SDF files.
            - static_df (pd.DataFrame): Static data from the report static CSV file.
    """
    # Define the root directory
    sdf_dir = rootdir / 'inputs' / 'ffiec_031_sdf_files'                                # Collect all .SDF files
    ls_rpt_files_paths = [file for file in sdf_dir.rglob("*.SDF")]                      # Append dataframes to a list
    ls_rpt_dfs = [pd.read_csv(file, sep=';') for file in ls_rpt_files_paths]            # Combine all dataframes into one
    hist_df = pd.concat(ls_rpt_dfs, axis=0, ignore_index=True)
    static_df_path = rootdir / 'inputs' / 'FFIEC_031_AX_Report_Static_Data.csv'
    static_df = pd.read_csv(static_df_path)                                           # Load static report data

    return hist_df, static_df


# Function to preprocess historical report files
def process_ffiec031_sdf_files(hist_data: pd.DataFrame, static_data: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess historical data by removing unnecessary fields, handling text values, and formatting MDRMs.
    Args:
        hist_data (pd.DataFrame): Historical data containing MDRMs and values.
        static_data (pd.DataFrame): Static data containing MDRM field names.
    Returns: pd.DataFrame: Cleaned and processed historical data.
    """
    # Drop unnecessary columns
    columns_to_drop = ['Bank RSSD Identifier', 'Last Update', 'Short Definition', 'Call Schedule', 'Line Number']
    hist_df_cleaned = hist_data.drop(columns=columns_to_drop, errors='ignore')

    # Remove '%' and convert 'Value' column to numeric, replacing errors with NaN
    hist_df_cleaned['Value'] = (hist_df_cleaned['Value'].astype(str).str.rstrip('%'))       # Remove '%' character
    hist_df_cleaned['Value'] = pd.to_numeric(hist_df_cleaned['Value'], errors='coerce')     # Coerce invalid values to NaN

    # Extract MDRMs to drop
    static_mdrms = static_data['field_name'].unique()
    string_mdrms = hist_df_cleaned[hist_df_cleaned['Value'].apply(lambda x: isinstance(x, str))]['MDRM #'].unique()
    text_mdrms = hist_df_cleaned[hist_df_cleaned['MDRM #'].str.contains('TEXT', na=False, regex=True)]['MDRM #'].unique()
    mdrms_to_drop = set(static_mdrms).union(string_mdrms, text_mdrms)

    # Filter out unwanted MDRMs and format types
    filtered_df = hist_df_cleaned[~hist_df_cleaned['MDRM #'].isin(mdrms_to_drop)]
    filtered_df['Value'] = pd.to_numeric(filtered_df['Value'], errors='coerce')
    filtered_df['Call Date'] = pd.to_datetime(filtered_df['Call Date'], format='%Y%m%d')

    # Prepare unique report dates
    report_dates = pd.DataFrame(
        pd.to_datetime(hist_data['Call Date'].unique(), format='%Y%m%d', errors='coerce'),
        columns=['ReportDate']
    ).dropna()

    # Extract and process MDRMs
    mdrm_dfs = []
    for mdrm in filtered_df['MDRM #'].unique():
        mdrm_df = (
            report_dates
            .merge(filtered_df[filtered_df['MDRM #'] == mdrm],
                   left_on='ReportDate', right_on='Call Date', how='outer')
        )
        mdrm_df['MDRM #'] = mdrm_df['MDRM #'].fillna(mdrm)
        mdrm_df.drop(columns=['Call Date'], inplace=True)
        mdrm_dfs.append(mdrm_df)

    # Combine all MDRM dataframes
    final_df = (
        pd.concat(mdrm_dfs, ignore_index=True)
        .rename(columns={'MDRM #': 'MDRM'})
        .sort_values(by=['MDRM', 'ReportDate'])
        .reset_index(drop=True)
    )

    return final_df


# Function to categorize MDRMs based on reporting frequency
def categorize_mdrms(proc_data: pd.DataFrame) -> Dict[str, List[str]]:
    """
        Categorizes MDRMs (Micro Data Reference Manual items) based on their reporting
        patterns and values across recent reporting periods.
        Args:
            proc_data (pd.DataFrame): The input DataFrame with at least the following columns:
                - 'MDRM': Unique identifier for each MDRM.
                - 'ReportDate': Date of the report.
                - 'Value': Numeric value associated with the MDRM for the given report date.
        Returns:
            Dict[str, List[str]]: A dictionary with keys representing reporting categories,
            and values being lists of MDRMs that fall into each category.
    """
    # Extract unique report dates
    unique_dates = sorted(proc_data['ReportDate'].unique(), reverse=True)

    # Filter report dates by frequency
    current_report_date = unique_dates[0]
    recent_8quarters = unique_dates[1:9]
    semi_annual_dates = [date for date in recent_8quarters if date.month in [6, 12]]
    annual_dates = [date for date in recent_8quarters if date.month == 12]

    # Filter historical data
    proc_data_wo_current = proc_data[proc_data['ReportDate'] != current_report_date]

    # Initialize MDRM lists
    zero_quarter_mdrms = []
    zero_quarter_novalue_mdrms = []
    zero_semi_annual_mdrms = []
    zero_annual_mdrms = []
    nonzero_annual_mdrms = []
    nonzero_semi_annual_mdrms = []
    nonzero_quarter_mdrms = []
    proc_mdrms = proc_data_wo_current['MDRM'].unique()

    # Classify MDRMs into MDRM lists
    for mdrm in proc_mdrms:
        mdrm_data = proc_data_wo_current[proc_data_wo_current['MDRM'] == mdrm]
        total_value = mdrm_data['Value'].sum(skipna=True)

        recent_8quarter_data = mdrm_data[mdrm_data['ReportDate'].isin(recent_8quarters)]
        recent_8quarter_value = recent_8quarter_data['Value'].sum(skipna=True)

        # Process zero balance MDRMs
        if total_value == 0 or recent_8quarter_value == 0:

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

        # Process non-zero balance MDRMs
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

    return mdrm_categories


# Function to generate metadata for line items
def generate_lineitem_metadata(hist_data: pd.DataFrame, proc_data: pd.DataFrame) -> pd.DataFrame:
    """
        Generates metadata for line items by combining historical MDRM data with
        processed categorization results to assign reporting frequencies.
        Args:
            hist_data (pd.DataFrame): Historical MDRM data.
            proc_data (pd.DataFrame): Processed data used to categorize MDRMs.
        Returns:
            pd.DataFrame: A DataFrame containing metadata for each MDRM.
    """
    # Get MDRM categories
    mdrm_categories = categorize_mdrms(proc_data)

    # Define reporting frequency categories
    def define_reporting_frequency(category: str) -> str:
        frequency_map = {
            'zero_quarter': 'Quarterly with Zeros',
            'zero_quarter_novalue': 'Quarterly - Not Reported',
            'zero_semi_annual': 'Semi-Annually with Zeros',
            'zero_annual': 'Annually with Zeros',
            'nonzero_quarter': 'Quarterly with Non-zeros',
            'nonzero_semi_annual': 'Semi-Annually with Non-zeros',
            'nonzero_annual': 'Annually with Non-zeros',
        }
        return frequency_map.get(category, 'Not Reported')

    # Filter and rename columns
    hist_data_filtered = hist_data[['MDRM #', 'Call Schedule', 'Line Number', 'Short Definition']].rename(
        columns={
            'MDRM #': 'MDRM',
            'Call Schedule': 'Schedule',
            'Line Number': 'LineNumber',
            'Short Definition': 'Definition'
        }
    )

    # Keep the first unique record for each MDRM
    hist_data_unique = hist_data_filtered.groupby('MDRM').first().reset_index()

    # Add reporting frequency column
    hist_data_unique["ReportingFrequency"] = 'Not Reported'

    # Populate the ReportingFrequency column based on MDRM categories
    for category, mdrms in mdrm_categories.items():
        hist_data_unique.loc[hist_data_unique["MDRM"].isin(mdrms), "ReportingFrequency"] = define_reporting_frequency(category)

    return hist_data_unique


# Connect to SQL Server with Open Database Connectivity (ODBC)
def upload_ffiec031_data_to_sqlserver(proc_data):
    """
    Outputs the final DataFrame to SQL Server and logs table updates.
    Args: proc_data (pd.DataFrame): The processed data to be uploaded to SQL Server.
    Returns:None
    """
    # Connection string for SQLAlchemy (if defined above, can be deleted)
    engine_str = (
        'mssql+pyodbc://host.name.company.com:15001/DBNAME'
        '?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    )
    engine = create_engine(engine_str)

    # Output final historical data into Audit SQL Dev server (overwrite the table)
    proc_data.to_sql(
        name='FFIEC031_HIST',       # Table name
        con=engine,                 # Database connection
        schema='dbo',               # Schema name
        if_exists='replace',        # Overwrite the table (use 'append' to add rows)
        index=False                 # Don't include the index as a column
    )

    # Write log files for table updates in SQL Server
    # Query table metadata
    metadata_query = """
        SELECT NAME, CREATE_DATE, MODIFY_DATE
        FROM SYS.OBJECTS
        WHERE NAME = 'FFIEC031_HIST'
    """

    tbl_metadata = pd.read_sql_query(metadata_query, engine)

    # Create/update log file
    logfile_dir = os.path.join(os.getcwd(), "Output")
    os.makedirs(logfile_dir, exist_ok=True)     # Ensure the output directory exists
    logfile_path = os.path.join(logfile_dir, "FFIEC031_Table_Update_Log.txt")

    # Prepare log message
    if not tbl_metadata.empty:
        log_msg = (
            f"Table '{tbl_metadata['Name'][0]}' was created at "
            f"{tbl_metadata['CREATE_DATE'][0]} and last modified at "
            f"{tbl_metadata['MODIFY_DATE'][0]}.\n"
        )
    else:
        log_msg = "No metadata found for table 'FFIEC031_HIST'.\n"

    # Write or append to the log file
    with open(logfile_path, 'a') as log_file:
        log_file.write(log_msg)

    print(f"Log has been updated at {logfile_path}.")


def create_output_folder() -> Path:
    """
    Creates the output folder if it doesn't already exist.
    Returns: Path - The path to the output folder.
    """
    output_folder = Path.cwd() / "outputs"
    output_folder.mkdir(exist_ok=True)
    return output_folder

def save_dataframe_to_excel(dataframe: pd.DataFrame, folder: Path, prefix: str, suffix: str, date_str: str):
    """
    Saves a DataFrame to an Excel file.
    Args:
        dataframe (pd.DataFrame): The DataFrame to save.
        folder (Path): The folder where the file will be saved.
        prefix (str): Prefix for the file name.
        suffix (str): Suffix for the file name.
        date_str (str): Timestamp for the file name.
    """
    file_path = folder / f"{prefix}_{suffix}_{date_str}.xlsx"
    dataframe.to_excel(file_path, index=False, engine='openpyxl')
    print(f"File saved as: {file_path}")

def main():
    """ Main function to process and save FFIEC 031 data and metadata. """
    try:
        # Initialize paths and configurations
        root_dir = Path.cwd()
        today_date = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        output_folder = create_output_folder()

        # Load and process data
        hist_df, static_df = load_ffiec031_data(root_dir)
        proc_df = process_ffiec031_sdf_files(hist_df, static_df)
        linemeta_df = generate_lineitem_metadata(hist_df, proc_df)

        # Define output file names
        save_dataframe_to_excel(
            dataframe=proc_df,
            folder=output_folder,
            prefix="FFIEC031",
            suffix="Processed",
            date_str=today_date
        )
        save_dataframe_to_excel(
            dataframe=linemeta_df,
            folder=output_folder,
            prefix="FFIEC031",
            suffix="Reference",
            date_str=today_date
        )

        # Optional: SQL Server upload (deactivated)
        # Uncomment and configure when ready
        # upload_ffiec031_data_to_sqlserver(proc_df)

        print("All operations completed successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()



