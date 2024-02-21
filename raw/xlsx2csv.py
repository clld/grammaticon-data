#!/usr/bin/env python3

import csv
from itertools import chain, repeat
from pathlib import Path

from openpyxl import load_workbook


def normalise_cell(value):
    if value is None:
        return ''
    else:
        return str(value).strip()


def pad_list(ls, width):
    if len(ls) == width:
        return ls
    elif len(ls) < width:
        return list(chain(ls, repeat('', width - len(ls))))
    else:
        raise ValueError(f'too long: {ls}')


def xlsx2csv(path):
    wb = load_workbook(filename=str(path), read_only=True, data_only=True)
    worksheets = wb.worksheets
    assert len(worksheets) == 1, f'{path}: not exactly 1 worksheet'
    sheet = worksheets[0]
    rows = [
        row_norm
        for row in sheet.iter_rows()
        if any(row_norm := [normalise_cell(cell.value) for cell in row])]

    table_width = max(len(row) for row in rows)
    with open(path.with_suffix('.csv'), 'w', encoding='utf-8') as f:
        wtr = csv.writer(f)
        wtr.writerows(pad_list(row, table_width) for row in rows)


def main():
    for p in Path(__file__).parent.glob('*.xlsx'):
        xlsx2csv(p)


if __name__ == '__main__':
    main()
