import pandas as pd

def convert_to_csv(input_file: str, output_file: str):
    # Detect file type from extension
    ext = input_file.split(".")[-1].lower()

    if ext == "csv":
        df = pd.read_csv(input_file)
    elif ext in ["xls", "xlsx"]:
        df = pd.read_excel(input_file)
    elif ext == "json":
        df = pd.read_json(input_file)
    elif ext == "parquet":
        df = pd.read_parquet(input_file)
    elif ext in ["txt", "dat"]:
        # assuming delimited text, update delimiter if needed
        df = pd.read_csv(input_file, delimiter="\t") 
    elif ext == "sql":
        with open(input_file, "r") as f:
            sql_script = f.read()
    
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    # Save as CSV
    df.to_csv(output_file, index=False)
    print(f"✅ Converted {input_file} → {output_file}")


# Example usage
convert_to_csv("02_NorthWind_Insert.sql", "output.csv")
