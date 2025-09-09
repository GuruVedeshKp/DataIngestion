# cli.py
import argparse
import yaml
import pandas as pd
from pathlib import Path
from ingest import read_file
from actions import apply_auto_fix, flatten_extra_columns  # add flatten_extra_columns

def classify_rows(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Classify rows into accepted, quarantine, violations.
    """
    df['status'] = 'accepted'

    # Mark missing required columns as violation
    required_cols = ['OrderID','CustomerID','EmployeeID','OrderDate','ShippedDate','ShipVia','Freight']
    for col in required_cols:
        df.loc[df[col].isna(), 'status'] = 'violation'

    # Example quarantine rule: negative freight or shipped date < order date
    df['Freight'] = pd.to_numeric(df['Freight'], errors='coerce')  # safely convert to numeric
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce')

    df.loc[df['Freight'] < 0, 'status'] = 'quarantine'
    df.loc[df['OrderDate'] > df['ShippedDate'], 'status'] = 'quarantine'

    accepted = df[df['status']=='accepted']
    quarantine = df[df['status']=='quarantine']
    violations = df[df['status']=='violation']

    return accepted, quarantine, violations

def main(data_path, config_path):
    # 1️⃣ Read file
    df = read_file(data_path)

    # 2️⃣ Flatten any extra/misaligned columns
    df = flatten_extra_columns(df)
    print(f"Total rows read: {len(df)}")  # verify all rows are preserved

    # 3️⃣ Load config
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f)

    # 4️⃣ Apply auto-fix
    df_fixed = apply_auto_fix(df, cfg)

    # 5️⃣ Classify rows
    accepted, quarantine, violations = classify_rows(df_fixed)

    # 6️⃣ Export results
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    accepted.to_csv(output_dir/'accepted.csv', index=False)
    quarantine.to_csv(output_dir/'quarantine.csv', index=False)
    violations.to_csv(output_dir/'violations.csv', index=False)

    print(f"Done. Accepted: {len(accepted)}, Quarantine: {len(quarantine)}, Violations: {len(violations)}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', required=True)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    main(args.data, args.config)

