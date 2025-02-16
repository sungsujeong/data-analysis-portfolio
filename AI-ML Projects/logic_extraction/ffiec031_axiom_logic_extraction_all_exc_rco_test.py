import re
import os
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
pd.set_option('max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


# Function that captures an index of strings in an Axiom log file
def find_string_line(filepath, search_string):
    ls_indexes = []
    with open(filepath, 'r', encoding='utf8') as file:
        for index, line in enumerate(file, start=1):
            if search_string in line:
                ls_indexes.append(index)
    return ls_indexes


# Function that extracts string lines by using indexes
def extract_lines_using_indexes(filepath, start_index, end_index):
    with open(filepath, 'r', encoding='utf8') as file:
        lines = file.readlines()
        return lines[start_index-1:end_index]


# Function that extracts texts where logic is stored
def extract_logic_texts(filepath):
    """
    Extract relevant logic texts from a log file.

    Args:
        filepath (str): Path to the log file.

    Returns:
        list: Cleaned-up logic text entries.
    """
    def clean_logic_texts(logic_texts):
        """Clean and flatten the list of logic texts."""
        cleaned = [
            lgc.replace('\t', '').replace('\n', '')
            for sublist in logic_texts for lgc in sublist
        ]
        return cleaned

    def extract_lines_between_indices(filepath, start_indices, end_indices):
        """
        Extract lines from a file based on start and end indices.
        """
        extracted = []
        for start, end in zip(start_indices, end_indices):
            extracted.append(extract_lines_using_indexes(filepath, start, end))
        return extracted

    # Step 1: Identify line indexes
    stmt_indexes = find_string_line(filepath, 'SQL statement: INSERT / ')
    patched_stmt_indexes = find_string_line(filepath, 'Patched SQL statement: INSERT / ')
    where_indexes = find_string_line(filepath, 'WHERE')
    join_indexes = find_string_line(filepath, 'JOIN')
    from_indexes = find_string_line(filepath, 'FROM')

    # Step 2: Determine the appropriate line groups
    if not stmt_indexes:
        return []  # No statements to process.

    # Filter statement indexes based on patched statements
    if patched_stmt_indexes:
        valid_stmt_indexes = [idx for idx in stmt_indexes if idx <= max(patched_stmt_indexes)]
    else:
        valid_stmt_indexes = stmt_indexes

    # Determine FROM, JOIN, or WHERE indexes
    if where_indexes:
        boundary_indexes = where_indexes
    elif join_indexes:
        boundary_indexes = join_indexes
    else:
        boundary_indexes = from_indexes

    # Step 3: Identify end indices for each start index
    end_indices = []
    end_index_iter = iter(boundary_indexes)
    for start in valid_stmt_indexes:
        for end in end_index_iter:
            if end >= start:
                end_indices.append(end)
                break

    # Step 4: Extract logic texts between determined indexes
    logic_texts = extract_lines_between_indices(filepath, valid_stmt_indexes, end_indices)

    # Step 5: Filter for "Patched SQL statements" if applicable
    final_logic_texts = [
        lgc_texts for lgc_texts in logic_texts
        if any(line.startswith('t\Patched SQL statement: INSERT /') for line in lgc_texts)
    ]

    # If no patched logic texts are found, fallback to all logic texts
    if not final_logic_texts:
        final_logic_texts = logic_texts

    # Step 6: Clean and flatten the final logic text entries
    return clean_logic_texts(final_logic_texts)


# Function that concatenates extracted logic texts
def concat_logic_texts(filepath):
    """
    Concatenate extracted logic texts into grouped entries.

    Groups logic texts by splitting at occurrences of 'SQL Statement: INSERT /'.

    Args:
        filepath (str): Path to the log file.

    Returns:
        list: A list of grouped logic texts, where each group is concatenated into a single string.
    """
    grouped_logic_texts = []
    current_group = []

    # Extract logic texts from the file
    for text in extract_logic_texts(filepath):
        # When encountering a new SQL statement, finalize the current group
        if 'SQL Statement: INSERT /' in text and current_group:
            grouped_logic_texts.append(' '.join(current_group))
            current_group = []

        # Add the current text to the group
        current_group.append(text)

    # Append the last group if it exists
    if current_group:
        grouped_logic_texts.append(' '.join(current_group))

    return grouped_logic_texts


# Function that extracts elements of logic from an Axiom log file
def extract_axiom_logic(filepath):
    def extract_parts(pattern, text, default="N/A"):
        """Extract parts based on the pattern or return default."""
        match = re.findall(pattern, text)
        return match[0].split('/') if match else [default]

    def extract_logic_parts(text, keyword):
        """Extract logic parts (e.g., SELECT-CASE, THEN-WHEN)."""
        if keyword in text:
            parts = text.split(keyword)
            return parts[1:] if len(parts) > 1 else []
        return []

    def normalize_list_lengths(lists):
        """Normalize the lengths of lists by padding with None."""
        max_length = max(len(lst) for lst in lists)
        for lst in lists:
            lst.extend([None] * (max_length - len(lst)))

    # Data structures
    ls_rpt_line, ls_mdrm, ls_elem_lgc, ls_upstrm_lgc, ls_mdl = [], [], [], [], []
    ls_mdl_temp, ls_axiom_data_cnt, ls_mdrm_elem = [], [], []

    # Process input file
    for logic_texts in concat_logic_texts(filepath):
        for text in logic_texts:
            # Extract MDRM and report line
            if 'DISTINCT' not in text and 'NOT EXISTS' not in text:
                parts = extract_logic_parts(text, 'SELECT')
                for part in parts:
                    rpt_line, mdrm = extract_parts(r'/[^()]*\/', part.strip(), default="N/A")[1:3]
                    ls_rpt_line.append(rpt_line)
                    ls_mdrm.append(mdrm)

            # Extract Element Logic
            if 'WHEN' in text:
                parts = extract_logic_parts(text, 'WHEN')
                for part in parts:
                    elem_logic = part.split('THEN')[0].strip()
                    ls_elem_lgc.append(elem_logic)

            # Extract Upstream Logic
            if 'WHERE' in text:
                parts = extract_logic_parts(text, 'WHERE')
                upstrm_logic = parts[0].strip() if parts else 'N/A'
                ls_upstrm_lgc.append(upstrm_logic)

            # Extract Model Information
            if 'FROM' in text:
                parts = extract_logic_parts(text, 'FROM')
                for part in parts:
                    mdl_part = part.split('WHERE')[0] if 'WHERE' in part else part
                    ls_mdl.append(mdl_part.strip())

    # Normalize list lengths
    normalize_list_lengths([ls_rpt_line, ls_mdrm, ls_elem_lgc, ls_upstrm_lgc, ls_mdl])

    # Create DataFrame
    logic_df = pd.DataFrame({
        'Line_Number': ls_rpt_line,
        'MDRM': ls_mdrm,
        'Axiom_Element_Logic': ls_elem_lgc,
        'Axiom_Upstream_Logic': ls_upstrm_lgc,
        'Axiom_Model': ls_mdl,
    })

    logic_df[['MDRM', 'Line_Number']] = logic_df[['MDRM', 'Line_Number']].astype(str)
    valid_df = logic_df[logic_df['MDRM'].str.len() == 8]

    # Process MDRM Element Mapping
    element_rows = []
    for _, row in valid_df.iterrows():
        elements = list(set(re.findall(r'\b\w+\.(\b[a-zA-Z_]\w*)', row['Axiom_Element_Logic'])))
        for elem in elements:
            element_rows.append([row['MDRM'], elem])

    element_df = pd.DataFrame(element_rows, columns=['MDRM', 'Axiom_Element'])
    final_df = valid_df.merge(element_df, on='MDRM', how='left')

    return final_df[['Line_Number', 'MDRM', 'Axiom_Element', 'Axiom_Element_Logic', 'Axiom_Upstream_Logic', 'Axiom_Model']]



# Helper function that creates a dataframe form an Excel file
def create_excel_from_dataframe(data, excel_file):
    ls_sheet_name = ['Axiom Logic']
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        data.to_excel(writer, sheet_name=ls_sheet_name[0], index=False)


# Helper function that creates a folder that stores output results
def output_folder():
    folder_path = os.getcwd()
    if not os.path.exists(folder_path + '\\outputs'):
        os.makedirs(folder_path + '\\outputs')
    return folder_path + '\\outputs'


# Helper function that locates a path of Axiom logic files
def find_filepaths(input_folder):
    """input_folder must be string"""
    rootdir = os.getcwd()
    ls_subdirs = []
    ls_filepaths = []
    for subdirs, dirs, files in os.walk(rootdir):
        ls_subdirs.append(subdirs)
    for subdir in ls_subdirs:
        for path in os.listdir(subdir):
            if input_folder in subdir:
                ls_filepaths.append(subdir+'\\'+path)
    return ls_filepaths


# Helper function that extracts a report name
def extract_report_name(input_folder):
    ls_filepaths = find_filepaths(input_folder)
    for path in ls_filepaths:
        rpt_name = path.split('\\')[-1].split('_')[1] + path.split('\\')[-1].split('_')[2]
    return rpt_name


# Helper function that creates a dataframe containing logic extraction results
def create_logic_dataframe(input_folder):
    """
    Processes log files in the input folder to generate a consolidated DataFrame
    containing extracted logic and metadata.

    Parameters:
        input_folder (str): Path to the folder containing log files.

    Returns:
        pd.DataFrame: A consolidated DataFrame with extracted logic and metadata.
    """
    # Gather all .log file paths grouped by schema name
    log_files_by_schema = {}
    for path in find_filepaths(input_folder):
        if path.endswith('.log'):
            schema_name = path.split('\\')[-1]
            log_files_by_schema.setdefault(schema_name, []).append(path)

    # Process each schema group
    schema_dataframes = []
    for schema_name, file_paths in log_files_by_schema.items():
        print(f"Processing schema: {schema_name}")

        for path in file_paths:
            # Extract logic from the log file
            logic_df = extract_axiom_logic(path)
            logic_df['Report'] = f"filepath_{schema_name}"  # Add report identifier
            logic_df['Schedule'] = logic_df['Line_Number'].str[:4]  # Derive schedule

            schema_dataframes.append(logic_df)

    # Combine all schema DataFrames
    if schema_dataframes:
        all_schemas_df = pd.concat(schema_dataframes, axis=0, ignore_index=True)

        # Move 'Report' and 'Schedule' columns to the front
        columns = ['Report', 'Schedule'] + [col for col in all_schemas_df.columns if col not in ['Report', 'Schedule']]
        all_schemas_df = all_schemas_df[columns]
    else:
        all_schemas_df = pd.DataFrame()

    return all_schemas_df


# Main function to orchestrate the Axiom logic extraction process
def main(input_folder):
    """
    Main function to extract logic from Axiom log files, save results to an Excel file,
    and organize the output.

    Args:
        input_folder (str): Path to the input folder containing Axiom log files.
    """
    try:
        # Ensure output folder exists
        output_path = output_folder()

        # Generate report name and timestamp
        report_name = extract_report_name(input_folder)
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        excel_file_path = os.path.join(
            output_path,
            f"{report_name.lower()}_axiom_log_file_logic_extraction_{current_time}.xlsx"
        )

        # Create logic DataFrame and save as Excel
        logic_df = create_logic_dataframe(input_folder)
        if logic_df.empty:
            print("No logic extracted. Please check the input folder for valid log files.")
        else:
            create_excel_from_dataframe(logic_df, excel_file_path)
            print(f"Excel file successfully created at: {excel_file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")


# Excecute main function
#if __name__ == '__main__':
#    main(input_folder)




















































