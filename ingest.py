# ingest.py
import pandas as pd
import csv
from pathlib import Path

NA_TOKENS = ['', 'NA', 'N/A', 'NULL', '<NA>']

def read_file(path: str) -> pd.DataFrame:
    """
    Read CSV/JSON/Parquet/Excel robustly, preserving all rows even if some rows 
    have extra/missing columns. Standardizes NA tokens into pandas NA.
    """
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix == '.csv':
        rows = []
        with open(p, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, quotechar='"')``
            headers = next(reader)
            for i, row in enumerate(reader):
                # Pad missing columns
                if len(row) < len(headers):
                    row += [''] * (len(headers) - len(row))
                # Merge extra columns into last column
                elif len(row) > len(headers):
                    row = row[:len(headers)-1] + [','.join(row[len(headers)-1:])]
                rows.append(row)

        df = pd.DataFrame(rows, columns=headers, dtype=str)

    elif suffix == '.json':
        df = pd.read_json(p, dtype=str)

    elif suffix == '.parquet':
        df = pd.read_parquet(p)
        # Ensure string dtype for consistency
        df = df.astype(str)

    elif suffix in ('.xls', '.xlsx'):
        df = pd.read_excel(p, dtype=str)

    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    # Normalize missing values across all file types
    df.replace(NA_TOKENS, pd.NA, inplace=True)

    return df

