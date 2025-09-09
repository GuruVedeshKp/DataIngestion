# actions.py
import pandas as pd
import re

EXPECTED_COLUMNS = [
    'OrderID','CustomerID','EmployeeID','OrderDate','ShippedDate',
    'ShipVia','Freight','ShipName','ShipAddress','ShipCity',
    'ShipRegion','ShipPostalCode','ShipCountry'
]

def flatten_extra_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    If CSV row has extra columns, join them into the last column to keep a scalar.
    """
    if len(df.columns) > len(EXPECTED_COLUMNS):
        extras = df.columns[len(EXPECTED_COLUMNS):]
        df[EXPECTED_COLUMNS[-1]] = df[EXPECTED_COLUMNS[-1]].astype(str) + ' ' + df[extras].astype(str).agg(' '.join, axis=1)
        df = df[EXPECTED_COLUMNS]
    return df

def apply_auto_fix(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Apply auto-corrections and type coercions according to rules.
    """
    df_fixed = flatten_extra_columns(df.copy())
    rules = cfg.get('rules', {})

    for field, rule in rules.items():
        # Fill missing values with default
        if 'default' in rule:
            df_fixed[field] = df_fixed[field].replace('', pd.NA).fillna(rule['default'])

        # Apply regex pattern
        pattern = rule.get('pattern')
        if pattern:
            df_fixed[field] = df_fixed[field].apply(
                lambda x: x if pd.isna(x) or re.match(pattern, str(x)) else pd.NA
            )

        # Type coercion
        dtype = rule.get('dtype')
        if dtype:
            if dtype == 'integer':
                df_fixed[field] = pd.to_numeric(df_fixed[field], errors='coerce')
                df_fixed[field] = df_fixed[field].apply(lambda x: x if pd.isna(x) or isinstance(x, (int,float)) else pd.NA)
                df_fixed[field] = df_fixed[field].astype('Int64')
            elif dtype == 'float':
                df_fixed[field] = pd.to_numeric(df_fixed[field], errors='coerce')
            elif dtype == 'string':
                df_fixed[field] = df_fixed[field].astype(str)

    return df_fixed

