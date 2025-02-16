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
    ls_stmt_idx = find_string_line(filepath, 'SQL statement: INSERT / ')
    ls_patd_stmt_idx = find_string_line(filepath, 'Patched SQL statement: INSERT / ')
    ls_from_join_where = []
    ls_start_idx = []
    if len(ls_stmt_idx) > 0:
        for idx in ls_stmt_idx:
            if len(ls_patd_stmt_idx) > 0:
                if idx in ls_patd_stmt_idx:
                    ls_start_idx = [idx for idx in ls_stmt_idx if idx <= max(ls_patd_stmt_idx)]
            else:
                ls_start_idx = ls_stmt_idx
        if len(find_string_line(filepath, 'WHERE')) > 0:
            ls_from_join_where = find_string_line(filepath, 'WHERE')
        else:
            if len(find_string_line(filepath, 'JOIN')) > 0:
                ls_from_join_where = find_string_line(filepath, 'JOIN')
            else:
                ls_from_join_where = find_string_line(filepath, 'FROM')

    ls_end_idx = []
    end_idx_iter = iter(ls_from_join_where)
    for start in ls_start_idx:
        for end in end_idx_iter:
            if end >= start:
                ls_end_idx.append(end)
                break

    ls_lgc_txts = []
    for start in enumerate(ls_start_idx):
        for end in enumerate(ls_end_idx):
            if start[0] == end[0]:
                ls_lgc_txts.append(extract_lines_using_indexes(filepath, start[1], end[1]))

    # Extract Patched SQL statements only when they exist
    ls_fnl_lgc = []
    for lgc_txts in ls_lgc_txts:
        for word in lgc_txts:
            if word.startswith('t\Patched SQL statement: INSERT /'):
                ls_fnl_lgc.append(lgc_txts)
                break
    if not ls_fnl_lgc:
        ls_fnl_lgc = ls_lgc_txts
    ls_fnl_lgc2 = [[lgc.replace('\t', '').replace('\n', '') for lgc in lgc_txts] for lgc_txts in ls_fnl_lgc]

    ls_fnl_lgc3 = []
    for fnl_lgc in ls_fnl_lgc2:
        if isinstance(fnl_lgc, list):
            ls_fnl_lgc3.extend(fnl_lgc)

    return ls_fnl_lgc3


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


# Function that extracts elements of logic from an Axiom log file
def extract_axiom_logic(filepath):
    global ls_rpt_line, ls_mdrm, ls_elem_lgc, ls_upstrm_lgc, ls_mdl, ls_mdl_temp, ls_axiom_data_cnt, ls_mdrm_elem
    ls_rpt_line = []
    ls_mdrm = []
    ls_elem_lgc = []
    ls_upstrm_lgc = []
    ls_mdl = []
    ls_mdl_temp = []
    ls_axiom_data_cnt = []

    for ls_txts in concat_logic_texts(filepath):
        for txt in ls_txts:

            # Report line items & MDRM
            if 'DISTINCT' not in txt:
                if 'CASE' in txt:
                    if txt.find(re.findall(r'\/[^()]*\/', txt)[0]) < txt.find('CASE'):
                        if re.findall(r'\/[^()]*\/', txt)[0] != '/*+ APPEND */' and re.findall(r'\/[^()]*\/', txt)[0] != '/*+ APPEND PARALLEL (8) */':
                            parts = txt.split('SELECT', 1)
                            for part in parts[1:]:
                                select_case_parts = part.split('CASE')
                                if len(list(filter(None, re.findall(r'\/[^()]*\/', select_case_parts[0])[0].split('/')))) == 2:
                                    rpt_line = re.findall(r'\/[^()]*\/', select_case_parts[0].strip())[0].split('/')[1]
                                    mdrm = re.findall(r'\/[^()]*\/', select_case_parts[0].strip())[0].split('/')[2]
                                    ls_rpt_line.append(rpt_line)
                                    ls_mdrm.append(mdrm)
                                elif len(list(filter(None, re.findall(r'\/[^()]*\/', select_case_parts[0])[0].split('/')))) > 2:
                                    rpt_line = re.findall(r'\/[^()]*\/', select_case_parts[0].strip())[0].split('/')[2]
                                    mdrm = re.findall(r'\/[^()]*\/', select_case_parts[0].strip())[0].split('/')[3]
                                    ls_rpt_line.append(rpt_line)
                                    ls_mdrm.append(mdrm)
                                else:
                                    rpt_line = 'N/A'
                                    mdrm = 'N/A'
                                    ls_rpt_line.append(rpt_line)
                                    ls_mdrm.append(mdrm)
                    else:
                        parts = txt.split('THEN')
                        for part in parts[1:]:
                            then_when_parts = part.split('WHEN')
                            if len(list(filter(None, re.findall(r'\/[^()]*\/', then_when_parts[0])[0].split('/')))) == 2:
                                rpt_line = re.findall(r'\/[^()]*\/', then_when_parts[0].strip())[0].split('/')[1]
                                mdrm = re.findall(r'\/[^()]*\/', then_when_parts[0].strip())[0].split('/')[2]
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                            elif len(list(filter(None, re.findall(r'\/[^()]*\/', then_when_parts[0])[0].split('/')))) > 2:
                                rpt_line = re.findall(r'\/[^()]*\/', then_when_parts[0].strip())[0].split('/')[2]
                                mdrm = re.findall(r'\/[^()]*\/', then_when_parts[0].strip())[0].split('/')[3]
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                            else:
                                rpt_line = 'N/A'
                                mdrm = 'N/A'
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                else:
                    if 'NOT EXISTS' not in txt:
                        parts = txt.split('SELECT')
                        for part in parts[1:]:
                            select_from_parts = part.split('FROM')
                            if len(list(filter(None, re.findall(r'\/[^()]*\/', select_from_parts[0])[0].split('/')))) == 2:
                                rpt_line = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[1]
                                mdrm = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[2]
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                            elif len(list(filter(None, re.findall(r'\/[^()]*\/', select_from_parts[0])[0].split('/')))) > 2:
                                rpt_line = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[2]
                                mdrm = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[3]
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                            else:
                                rpt_line = 'N/A'
                                mdrm = 'N/A'
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                    else:
                        parts = txt.split('SELECT', 1)
                        for part in parts[1:]:
                            select_from_parts = part.split('FROM', 1)
                            if len(list(filter(None, re.findall(r'\/[^()]*\/', select_from_parts[0])[0].split('/')))) == 2:
                                rpt_line = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[1]
                                mdrm = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[2]
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                            elif len(list(filter(None, re.findall(r'\/[^()]*\/', select_from_parts[0])[0].split('/')))) > 2:
                                rpt_line = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[2]
                                mdrm = re.findall(r'\/[^()]*\/', select_from_parts[0].strip())[0].split('/')[3]
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)
                            else:
                                rpt_line = 'N/A'
                                mdrm = 'N/A'
                                ls_rpt_line.append(rpt_line)
                                ls_mdrm.append(mdrm)

            # Element logic
            if 'WHEN' in txt:
                parts = txt.split('WHEN')
                for part in parts[1:]:
                    when_then_parts = part.split('THEN')
                    if len(when_then_parts) > 0:
                        elem_lgc = when_then_parts[0].strip()
                        ls_elem_lgc.append(elem_lgc)

            else:
                elem_lgc = 'N/A'
                ls_elem_lgc.append(elem_lgc)

            # Upper node logic
            if 'WHEN' in txt:
                upstrm_lgc_cnt = txt.count('WHEN')
                if 'WHERE' in txt:
                    parts = txt.split('WHERE')
                    for part in parts[1:]:
                        upstrm_lgc = part.strip()
                        ls_upstrm_lgc.append(upstrm_lgc)
                        ls_upstrm_lgc = [upstrm_lgc for upstrm_lgc in ls_upstrm_lgc for _ in range(upstrm_lgc_cnt)]
                else:
                    upstrm_lgc = 'N/A'
                    ls_upstrm_lgc.append(upstrm_lgc)
                    ls_upstrm_lgc = [upstrm_lgc for upstrm_lgc in ls_upstrm_lgc for _ in range(upstrm_lgc_cnt)]
            else:
                if 'NOT EXISTS' not in txt:
                    if 'WHERE' in txt:
                        parts = txt.split('WHERE')
                        for part in parts[1:]:
                            upstrm_lgc = part.strip()
                            ls_upstrm_lgc.append(upstrm_lgc)
                    else:
                        upstrm_lgc = 'N/A'
                        ls_upstrm_lgc.append(upstrm_lgc)
                else:
                    txt = txt[:txt.fin('NOT EXISTS')]
                    if 'WHERE' in txt:
                        parts = txt.split('WHERE')
                        for part in parts[1:]:
                            upstrm_lgc = part.strip()
                            ls_upstrm_lgc.append(upstrm_lgc)
                    else:
                        upstrm_lgc = 'N/A'
                        ls_upstrm_lgc.append(upstrm_lgc)

            # Model info
            if 'WHEN' in txt:
                mdl_cnt = txt.count('WHEN')
                if 'JOIN' not in txt:
                    parts = txt.split('FROM')
                    for part in parts[1:]:
                        if 'WHERE' in part:
                            from_where_parts = parts.split('WHERE')
                            if len(from_where_parts) > 0:
                                mdl = from_where_parts[0].strip()
                                ls_mdl.append(mdl)
                                ls_mdl = [mdl for mdl in ls_mdl for _ in range(mdl_cnt)]
                        else:
                            from_parts = part.split(' ')
                            if len(from_parts) > 0:
                                mdl = from_where_parts[0].strip()
                                ls_mdl.append(mdl)
                                ls_mdl = [mdl for mdl in ls_mdl for _ in range(mdl_cnt)]
                else:
                    parts = txt.split('FROM')
                    for part in parts[1:]:
                        ls_axiom_data_cnt.append(part.count('AXIOM_DATA'))
                        from_join_parts = part.split('JOIN')
                        if len(from_join_parts) > 0:
                            mdl = from_join_parts[0].strip()
                            ls_mdl_temp.append(mdl)

                    for part in from_join_parts[1:]:
                        join_on_parts = part.split('ON')
                        if len(join_on_parts) > 0:
                            mdl = join_on_parts[0].strip()
                            ls_mdl_temp.append(mdl)

                    start_idx = 0
                    for cnt in ls_axiom_data_cnt:
                        end_idx = start_idx + cnt
                        curr_grp = ls_mdl_temp[start_idx:end_idx]
                        comb_mdl = ', '.join(curr_grp)
                        start_idx = end_idx

                    ls_mdl.append(comb_mdl)
                    ls_mdl = [mdl for mdl in ls_mdl for _ in range(mdl_cnt)]

            else:
                if 'NOT EXISTS' not in txt:
                    if 'JOIN' not in txt:
                        parts = txt.split('FROM')
                        for part in parts[1:]:
                            if 'WHERE' in part:
                                from_where_parts = part.split('WHERE')
                                if len(from_where_parts) > 0:
                                    mdl = from_where_parts[0].strip()
                                    ls_mdl.append(mdl)
                            else:
                                from_parts = part.split(' ')
                                if len(from_parts) > 0:
                                    mdl = from_where_parts[0].strip()
                                    ls_mdl.append(mdl)

                    else:
                        parts = txt.split('FROM')
                        for part in parts[1:]:
                            ls_axiom_data_cnt.append(part.count('AXIOM_DATA'))
                            from_join_parts = part.split('JOIN')
                            if len(from_join_parts) > 0:
                                mdl = from_join_parts[0].strip()
                                ls_mdl_temp.append(mdl)

                        for part in from_join_parts[1:]:
                            join_on_parts = parts.split('ON')
                            if len(join_on_parts) > 0:
                                mdl = join_on_parts[0].strip()
                                ls_mdl_temp.append(mdl)

                        start_idx = 0
                        for cnt in ls_axiom_data_cnt:
                            end_idx = start_idx + cnt
                            curr_grp = ls_mdl_temp[start_idx:end_idx]
                            comb_mdl = ', '.join(curr_grp)
                            start_idx = end_idx
                        ls_mdl.append(comb_mdl)

                else:
                    txt = txt[:txt.find('NOT EXISTS')]
                    if 'JOIN' not in txt:
                        parts = txt.split('FROM')
                        for part in parts[1:]:
                            if 'WHERE' in part:
                                from_where_parts = part.split('WHERE')
                                if len(from_where_parts) > 0:
                                    mdl = from_where_parts[0].strip()
                                    ls_mdl.append(mdl)
                            else:
                                from_parts = part.split(' ')
                                if len(from_parts) > 0:
                                    mdl = from_where_parts[0].strip()
                                    ls_mdl.append(mdl)

                    else:
                        parts = txt.split('FROM')
                        for part in parts[1:]:
                            ls_axiom_data_cnt.append(part.count('AXIOM_DATA'))
                            from_join_parts = part.split('JOIN')
                            if len(from_join_parts) > 0:
                                mdl = from_join_parts[0].strip()
                                ls_mdl_temp.append(mdl)

                        for part in from_join_parts[1:]:
                            join_on_parts = part.split('ON')
                            if len(join_on_parts) > 0:
                                mdl = join_on_parts[0].strip()
                                ls_mdl_temp.append(mdl)

                        start_idx = 0
                        for cnt in ls_axiom_data_cnt:
                            end_idx = start_idx + cnt
                            curr_grp = ls_mdl_temp[start_idx:end_idx]
                            comb_mdl = ', '.join(curr_grp)
                            start_idx = end_idx
                        ls_mdl.append(comb_mdl)

    max_length = max(len(ls_rpt_line), len(ls_mdrm), len(ls_elem_lgc), len(ls_upstrm_lgc), len(ls_mdl))
    ls_lists = [ls_rpt_line, ls_mdrm, ls_elem_lgc, ls_upstrm_lgc, ls_mdl]

    for ls in ls_lists:
        ls.extend([None] * (max_length - len(ls)))

    lgc_df = pd.DataFrame({'Line_Number': ls_rpt_line,
                           'MDRM': ls_mdrm,
                           'Axiom_Element_Logic': ls_elem_lgc,
                           'Axiom_Upstream_Logic': ls_upstrm_lgc,
                           'Axiom_Model': ls_mdl})
    lgc_df2 = lgc_df[(lgc_df['Line_Number'] != 'z_orphans') & (lgc_df['MDRM'].str.len() == 8)]

    # Extract elements
    ls_mdrm2 = lgc_df2['MDRM'].to_list()
    ls_elem_lgc2 = lgc_df2['Axiom_Element_Logic'].to_list()
    ls_mdrm_elem = []
    ls_mdrm_elem.append(['MDRM', 'Axiom_Element_Logic', 'Axiom_Element'])
    for i, mdrm in enumerate(ls_mdrm2):
        for j, elem_lgc in enumerate(ls_elem_lgc2):
            if i == j:
                mdrm = mdrm
                elem_lgc = elem_lgc
                ls_elems = list(set(re.findall(r'\b\w+\.(\b[a-zA-Z_]\w*)', elem_lgc)))
                ls_mdrm_elem.append([mdrm, elem_lgc, ls_elems])
    rows = []
    for i, item in enumerate(ls_mdrm_elem[1:]):
        for elem in item[2]:
            row = [item[0], item[1], elem]
            rows.append(row)
    elem_df = pd.DataFrame(rows, columns=ls_mdrm_elem[0]).drop(columns=['Axiom_Element_Logic'])
    lgc_df3 = lgc_df2.merge(elem_df, on='MDRM', how='left')[['Line_Number', 'MDRM', 'Axiom_Element', 'Axiom_Element_Logic', 'Axiom_Upstream_Logic', 'Axiom_Model']]

    return lgc_df3


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




















































