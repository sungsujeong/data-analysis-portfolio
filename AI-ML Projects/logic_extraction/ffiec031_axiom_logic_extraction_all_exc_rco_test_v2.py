import re
import os
import pandas as pd
from datetime import datetime
from typing import List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')
pd.set_option('max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


# Function that captures an index of strings in an Axiom log file
def find_string_lines(filepath: str, search_string: str, case_sensitive: bool = False, max_results: Optional[int] = None) -> List[int]:
    """
    Find line numbers in a file containing a specified string.

    Args:
        filepath (str): Path to the file to search.
        search_string (str): String to search for in each line.
        case_sensitive (bool, optional): Whether the search should be case-sensitive. Defaults to False.
        max_results (int, optional): Maximum number of results to return. If None, returns all matches.

    Returns:
        List[int]: List of line numbers (1-indexed) where the search string was found.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        PermissionError: If the user doesn't have permission to read the file.
    """
    line_numbers = []
    pattern = re.compile(re.escape(search_string), re.IGNORECASE if not case_sensitive else 0)

    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, start=1):
                if pattern.search(line):
                    line_numbers.append(line_number)
                    if max_results and len(line_numbers) >= max_results:
                        break
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        raise
    except PermissionError:
        print(f"Error: Permission denied to read file '{filepath}'.")
        raise

    return line_numbers


# Function that extracts string lines by using indexes
def extract_lines_using_indexes(filepath, start_index, end_index):
    with open(filepath, 'r', encoding='utf8') as file:
        lines = file.readlines()
        return lines[start_index-1:end_index]


def extract_logic_texts(filepath: str) -> List[str]:
    """
    Extract logic texts from SQL statements in a file.

    Args:
        filepath (str): Path to the file containing SQL statements.

    Returns:
        List[str]: Extracted and cleaned logic texts.
    """

    def find_string_lines(search_string: str) -> List[int]:
        return find_string_lines(filepath, search_string)

    def extract_range(start: int, end: int) -> List[str]:
        return extract_lines_using_indexes(filepath, start, end)

    stmt_indices = find_string_lines('SQL statement: INSERT / ')
    patched_stmt_indices = find_string_lines('Patched SQL statement: INSERT / ')

    if not stmt_indices:
        return []

    start_indices = [idx for idx in stmt_indices if idx <= max(patched_stmt_indices)] if patched_stmt_indices else stmt_indices

    end_keywords = ['WHERE', 'JOIN', 'FROM']
    end_indices = next((find_string_lines(kw) for kw in end_keywords if find_string_lines(kw)), [])

    if not end_indices:
        return []

    logic_texts = []
    for start, end in zip(start_indices, (end for end in end_indices if end >= start_indices[0])):
        logic_texts.append(extract_range(start, end))

    final_logic = [
                      texts for texts in logic_texts
                      if any(word.startswith('Patched SQL statement: INSERT /') for word in texts)
                  ] or logic_texts

    cleaned_logic = [
        [re.sub(r'[\t\n]', '', line) for line in texts if line.strip()]
        for texts in final_logic
    ]

    return [line for sublist in cleaned_logic for line in sublist]



# Function that concatenates extracted logic texts
def concat_logic_texts(filepath):
    ls_lgc_txts = []
    curr_txt_grp = []
    for txt in extract_logic_texts(filepath):
        if 'SQL Statement: INSERT /' in txt and curr_txt_grp:
            ls_lgc_txts.append([' '.join(curr_txt_grp)])
            curr_txt_grp = []
        curr_txt_grp.append(txt)
    if curr_txt_grp:
        ls_lgc_txts.append([' '.join(curr_txt_grp)])
    return ls_lgc_txts


import re
import pandas as pd

# Function to extract logic data
def extract_axiom_logic(filepath):
    """
    Extracts Axiom logic elements, including MDRM, Element Logic, and Model Information.
    Args:
        filepath (str): Path to the Axiom log file.
    Returns:
        DataFrame: A Pandas DataFrame containing the extracted logic data.
    """
    logic_lists = {
        "Line_Number": [],
        "MDRM": [],
        "Element_Logic": [],
        "Upstream_Logic": [],
        "Model": [],
    }

    # Define reusable regex patterns
    regex_pattern = re.compile(r'\/[^()]*\/')

    try:
        for texts in concat_logic_texts(filepath):
            for text in texts:
                process_report_lines(text, logic_lists, regex_pattern)
                process_element_logic(text, logic_lists)
                process_upstream_logic(text, logic_lists)
                process_model_info(text, logic_lists)
    except Exception as e:
        raise ValueError(f"Error processing file: {e}")

    # Normalize list lengths
    normalize_lengths(logic_lists)

    # Create the DataFrame
    lgc_df = pd.DataFrame(logic_lists)
    lgc_df_filtered = lgc_df[
        (lgc_df['Line_Number'] != 'z_orphans') & (lgc_df['MDRM'].str.len() == 8)
    ]

    # Extract elements and create a merged DataFrame
    final_df = extract_elements_and_merge(lgc_df_filtered)
    return final_df


def process_report_lines(text, logic_lists, regex_pattern):
    """Extract report lines and MDRM from text."""
    if 'DISTINCT' not in text and 'CASE' in text:
        match = regex_pattern.findall(text)
        if match:
            logic_lists["Line_Number"].append(match[0].split('/')[1])
            logic_lists["MDRM"].append(match[0].split('/')[2])
        else:
            logic_lists["Line_Number"].append("N/A")
            logic_lists["MDRM"].append("N/A")


def process_element_logic(text, logic_lists):
    """Extract element logic from text."""
    if 'WHEN' in text:
        parts = text.split('WHEN')
        for part in parts[1:]:
            when_then_parts = part.split('THEN')
            logic_lists["Element_Logic"].append(when_then_parts[0].strip() if when_then_parts else "N/A")
    else:
        logic_lists["Element_Logic"].append("N/A")


def process_upstream_logic(text, logic_lists):
    """Extract upstream logic from text."""
    if 'WHEN' in text and 'WHERE' in text:
        parts = text.split('WHERE')
        logic_lists["Upstream_Logic"].extend([parts[1].strip()] * text.count('WHEN'))
    else:
        logic_lists["Upstream_Logic"].append("N/A")


def process_model_info(text, logic_lists):
    """Extract model information from text."""
    if 'FROM' in text:
        parts = text.split('FROM')
        model_part = parts[1].split(' ')[0] if len(parts) > 1 else "N/A"
        logic_lists["Model"].append(model_part.strip())


def normalize_lengths(logic_lists):
    """Normalize lengths of all lists in the logic dictionary."""
    max_length = max(len(v) for v in logic_lists.values())
    for key in logic_lists.keys():
        logic_lists[key].extend([None] * (max_length - len(logic_lists[key])))


def extract_elements_and_merge(lgc_df):
    """Extract elements and merge with the logic DataFrame."""
    lgc_df['Elements'] = lgc_df['Element_Logic'].apply(lambda x: list(set(re.findall(r'\b\w+\.(\b[a-zA-Z_]\w*)', x))) if x else [])
    expanded_rows = [
        [row.Line_Number, row.MDRM, element, row.Element_Logic, row.Upstream_Logic, row.Model]
        for row in lgc_df.itertuples()
        for element in row.Elements
    ]
    final_df = pd.DataFrame(expanded_rows, columns=['Line_Number', 'MDRM', 'Element', 'Element_Logic', 'Upstream_Logic', 'Model'])
    return final_df


# Helper function that creates a dataframe form an Excel file
def create_excel_from_dataframe(data, excel_file):
    ls_sheet_name = ['Axiom Logic']
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        data.to_excel(writer, sheet_name=ls_sheet_name[0], index=False)


# Helper funtion that creates a folder that stores output results
def output_folder():
    folder_path = os.getcwd()
    if not os.path.exists(folder_path + '\\outputs'):
        os.makedirs(folder_path + '\\outputs')
    return folder_path + '\\outputs'


# Helper function that locates a path of Axiom logc files
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
    ls_schs = []
    dict_ls_schs = {}
    for path in find_filepath(input_folder):
        sch = path.split('/')[-1]
        if not sch.endswith(input_folder) and path[-4:] == '.log':
            ls_schs.append(sch)

    ls_schs = list(set(ls_schs))
    for sch in ls_schs:
        key = f"filepath_{sch}"
        dict_ls_schs[key] = []
        for path in find_filepath(input_folder):
            if sch in path.split('/')[-1] and path[-4:] == '.log':
                dict_ls_schs[key].append(path)

    ls_sch_dfs = []
    for key in dict_ls_schs.keys():
        print(key)
        for path in dict_ls_schs[key]:
            df = extract_axiom_logic(path)
            df['Report'] = key
            df['Schedule'] = df['Line_Number'].str[:4]
            ls_sch_dfs.append(df)

        all_schs_df = pd.concat(ls_sch_dfs, axis=0, ignore_index=True)
        ls_cols = all_schs_df.columns.tolist()
        new_cols = ls_cols[-2:] + ls_cols[:-2]
        all_schs_df = all_schs_df[new_cols]

    return all_schs_df


# Main function to orchestrate the Axiom logic extraction process
def main(input_folder):
    global ls_subidrs
    location = output_folder()
    rpt_name = extract_report_name()
    today_date = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    excel_file_path = location + rpt_name.lower() + '_axiom_log_file_logic_extraction_' + today_date + '.xlsx'
    create_excel_from_dataframe(create_logic_dataframe(input_folder), excel_file_path)


# Excecute main function
#if __name__ == '__main__':
#    main(input_folder)











































