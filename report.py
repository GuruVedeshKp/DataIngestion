import pandas as pd
from pathlib import Path

def export_violations(viol_df: pd.DataFrame, out_path: str):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    viol_df.to_csv(out_path, index=False)

def quarantine_records(df: pd.DataFrame, viol_df: pd.DataFrame, out_path: str):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    bad_idx = viol_df['row_index'].unique().tolist()
    df.loc[bad_idx].to_csv(out_path, index=False)
    return df.drop(index=bad_idx)
