import yaml
import pandas as pd
import re
from typing import Dict, List

def load_config(path: str) -> Dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def validate_rowwise(df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    """
    Returns a DataFrame `violations` with columns:
    row_index, field, rule, severity, message
    """
    violations = []
    fields_cfg = cfg.get('fields', {})
    for idx, row in df.iterrows():
        for field, rules in fields_cfg.items():
            val = row.get(field, None)
            # nullability
            if (pd.isna(val) or val == '') and not rules.get('nullable', True):
                violations.append((idx, field, 'nullability', 'ERROR', 'Null not allowed'))
                continue
            if pd.isna(val):
                continue
            # type checks (simple)
            expected = rules.get('dtype')
            try:
                if expected == 'integer':
                    int(val)
                elif expected == 'float':
                    float(val)
                elif expected == 'string':
                    str(val)
            except Exception:
                violations.append((idx, field, 'dtype', 'ERROR', f'Cannot coerce to {expected}'))
            # regex
            regex = rules.get('regex')
            if regex and not re.match(regex, str(val)):
                violations.append((idx, field, 'regex', 'ERROR', f'Value {val} not match {regex}'))
            # allowed_values
            allowed = rules.get('allowed_values')
            if allowed and val not in allowed:
                violations.append((idx, field, 'allowed_values', 'WARN', f'{val} not in allowed list'))
            # max_length
            ml = rules.get('max_length')
            if ml and isinstance(val, str) and len(val) > ml:
                violations.append((idx, field, 'max_length', 'WARN', f'Len {len(val)} > {ml}'))
    viol_df = pd.DataFrame(violations, columns=['row_index','field','rule','severity','message'])
    return viol_df

def check_uniqueness(df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    pk = cfg.get('primary_key')
    issues=[]
    if pk and pk in df.columns:
        dupes = df[df.duplicated(pk, keep=False)]
        for idx in dupes.index:
            issues.append((idx, pk, 'unique', 'ERROR', 'Duplicate primary key'))
    return pd.DataFrame(issues, columns=['row_index','field','rule','severity','message'])
