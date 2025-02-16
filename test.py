with pd.ExcelWrtier(file_to_save, engine='openpyxl') as wrtier:

    headers.to_excel(writer, sheet_name='lead_sheet', header=False, index=False)
    lead_sheet_temp.to_excel(writer, sheet_name='lead_sheet', header=False, index=False, startrow=4)

    wb = writer.book
    ws = writer.sheets['lead_sheet']

    mapping_cells = [
        ['A1:G1', 1, 'FFFFFF'],
        ['H1:N1', 8, 'C0C0C0']
    ]

    cols_to_date = [8, 9]
    for col in cols_to_date:
        for row in ws.iter_rows(min_col=col, max_col=col, min_row=5, max_row=ws.max_row):
            for cell in row:
                cell.number_format = 'YYYY-MM-DD'

    cols_to_ccy = [10, 11, 13, 35, 36]
    for col in cols_to_ccy:
        for row in ws.iter_rows(min_col=col, max_col=col, min_row=5, max_row=ws.max_row):
            for cell in row:
                cell.number_format = numbers.FORMAT_CURRENCY_USD_SIMPLE

    cols_to_pct = [38, 39, 40]
    for col in cols_to_pct:
        for row in ws.iter_rows(min_col=col, max_col=col, min_row=5, max_row=ws.max_row):
            for cell in row:
                cell.number_format = "0.0000%"

    for cell in mapping_cells:
        ws.merge_cells(cell[0])
        ws.cell(row=1, column=cell[1]).alignment = Alignment(horizontal='center')
        ws.cell(row=1, column=cell[1]).fill = PatternFill(start_color=cell[2], end_color=cell[2], fill_type='solid')

    for row_idx in [2, 3, 4]:
        for col_idx in range(1, ws.max_column + 1):
            ws.cell(row=row_idx, column=col_idx).alignment = Alignment(horizontal='center')

    for col_idx in range(1, ws.max_column + 1):
        ws.cell(row=2, column=col_idx).alignment = Alignment(wrap_text=True, horizontal='center')
        ws.cell(row=3, column=col_idx).fill = PatternFill(start_color='001F3F', end_color='001F3F', fill_type='solid')
        ws.cell(row=3, column=col_idx).font = Font(color='FFFFFF', bold=True)
        ws.cell(row=4, column=col_idx).font = Font(bold=True)
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = 20

    ws.row_dimensions[2].height = 120

    thin_border = Border(
        left=Side(style='thin', color='000000'),
        right=Side(style='thin', color='000000'),
        top=Side(style='thin', color='000000'),
        bottom=Side(style='thin', color='000000')
    )

    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.border = thin_border

    ws.freeze_panes = ws['B5']

    for row in ws.iter_rows():
        for cell in row:
            cell.protection = Protection(locked=False)

    cols_to_lock = [1, 3, 4, 5, 8, 9, 10]
    for col in cols_to_lock:
        for row in ws.iter_rows(min_col=col, max_col=col, min_row=5, max_row=ws.max_row):
            for cell in row:
                cell.fill = PatternFill(start_color='f0ffff', end_color='f0ffff', fill_type='solid')
                cell.protection = Protection(locked=True)

    for row in ws.iter_rows(min_row=1, max_row=4, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.protection = Protection(locked=False)

    ws.auto_filter.ref = 'A4:BH4'

    ws.protection.sheet = True
    ws.protection.enable()

    wb.save(file_to_save)