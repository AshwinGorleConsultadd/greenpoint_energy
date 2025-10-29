"""
enr_parser_v2.py

Purpose:
  Extract and clean ENR Top Firms table with accurate column mapping
  based on provided structure.

Output:
  List of dictionaries with clean, typed values.
"""

import pandas as pd
import re
from typing import List, Dict, Optional

try:
    import camelot
except Exception:
    camelot = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None


# ---------- Utilities ----------
def _parse_number(value: Optional[str]) -> Optional[float]:
    """Convert strings like '20,241.4' or '-' to float."""
    if not value or str(value).strip() in ["-", "—", ""]:
        return None
    s = str(value).replace(",", "").strip()
    try:
        return float(s)
    except:
        return None


def _clean_firm_name(value: str) -> str:
    """Clean firm name by removing † or trailing commas, etc."""
    if not isinstance(value, str):
        return ""
    value = re.sub(r"[\u2020†]", "", value)  # remove dagger symbols
    return value.strip().rstrip(",")


# ---------- Table extraction ----------
def extract_tables(pdf_path: str, pages: str = "1-end") -> List[pd.DataFrame]:
    """Try Camelot first, then fallback to pdfplumber."""
    dfs = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for t in tables:
                    df = pd.DataFrame(t)
                    if len(df) > 5:
                        dfs.append(df)
    except Exception:
        pass
    if not dfs:
        raise RuntimeError("No tables found in PDF.")
    return dfs

# ---------- Normalization ----------
def normalize_enr_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the ENR table.
    Columns (approx order):
    rank_2025, rank_2024, firm, revenue_m, intl_revenue_m, new_contracts_m,
    general_building_pct, manufacturing_pct, power_pct,
    water_waste_pct, industrial_petroleum_pct, transportation_pct,
    hazardous_waste_pct, telecom_pct, cm_at_risk_pct
    """
    
    # If columns are already set (from combine step), skip header identification
    # Check if columns look like they're already normalized
    column_names = df.columns.tolist()
    
    # Check if header rows are still present (non-string column names)
    if not isinstance(df.columns[0], str) or any(c in str(col).lower() for col in df.columns for c in ['firm', 'revenue', 'rank'] if isinstance(col, str)):
        # Identify header row
        header_row = None
        for i in range(min(3, len(df))):
            joined = " ".join(map(str, df.iloc[i].tolist())).lower()
            if "firm" in joined and "revenue" in joined:
                header_row = i
                break
        if header_row is not None:
            # Skip header rows
            df = df.iloc[header_row + 1:].reset_index(drop=True)
    
    # Rename columns if they're not already named properly
    if len(df.columns) > 0 and ("rank" in str(df.columns[0]).lower() or "firm" in str(df.columns[2]).lower() or not "rank_2025" in str(df.columns[0]).lower()):
        # Define clean column names (14 expected cols)
        clean_cols = [
            "enr_rank_2025",
            "enr_rank_2024",
            "firm",
            "total_revenue_m",
            "int_total_revenue_m",
            "new_contracts",
            "general_building_pct",
            "manufacturing_pct",
            "power_pct",
            "water_supply_pct",
            "sewer_waste_pct",
            "industrial_oilgas_pct",
            "transportation_pct",
            "hazardous_waste_pct",
            "telecom_pct",
        ]
        # Only rename if we have the expected number of columns or close to it
        if abs(len(df.columns) - len(clean_cols)) <= 5:
            df.columns = clean_cols[:len(df.columns)]

    # Clean numeric columns
    num_cols = [
        "enr_rank_2025",
        "enr_rank_2024",
        "total_revenue",
        "int_total_revenue",
        "new_contracts_m",
        "general_building_pct",
        "manufacturing_pct",
        "power_pct",
        "water_supply_pct",
        "sewer_waste_pct",
        "industrial_oilgas_pct",
        "transportation_pct",
        "hazardous_waste_pct",
        "telecom_pct",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].apply(_parse_number)

    # Clean firm name
    df["firm"] = df["firm"].apply(_clean_firm_name)

    # Drop empty rows
    df = df[df["firm"].notna() & (df["firm"] != "")]
    return df


def enr_table_to_dicts(df: pd.DataFrame) -> List[Dict]:
    """Convert DataFrame rows to list of dictionaries."""
    return df.to_dict(orient="records")


# ---------- Pipeline ----------
def parse_enr_pdf(pdf_path: str, pages: str = "all") -> List[Dict]:
    dfs = extract_tables(pdf_path, pages)
    
    # Debug: print what tables we found
    print(f"Found {len(dfs)} table(s)")
    
    # Find all tables that belong to the ENR table (they should have similar structure)
    enr_tables = []
    for i, df in enumerate(dfs):
        try:
            flat_text = " ".join(df.astype(str).values.flatten()).lower()
            # Check for key indicators
            has_firm = "firm" in flat_text
            has_revenue = "revenue" in flat_text or "mil" in flat_text
            has_rank = "rank" in flat_text or "2025" in flat_text or "2024" in flat_text
            has_companies = any(word in flat_text for word in ["aecom", "jacobs", "fluor", "dallas", "texas"])
            has_data_rows = len(df) > 10  # Has enough rows
            
            # Check if this looks like part of the ENR table
            if (has_firm or has_companies or has_rank or has_revenue) and has_data_rows:
                enr_tables.append(df)
                print(f"Table {i+1}: shape {df.shape} - included as part of ENR table")
        except Exception as e:
            print(f"Error checking table {i+1}: {e}")
            continue
    
    if not enr_tables:
        if dfs:
            # Fallback: use all tables that have reasonable size
            enr_tables = [df for df in dfs if len(df) > 10]
            if enr_tables:
                print(f"Using {len(enr_tables)} tables as fallback")
            else:
                raise RuntimeError("Could not find ENR table.")
        else:
            raise RuntimeError("Could not find ENR table.")
    
    print(f"\nCombining {len(enr_tables)} table(s) into master table...")
    
    # Find which table has the header (search for "FIRM" in the first few rows)
    header_table_idx = None
    for i, df in enumerate(enr_tables):
        for row_idx in range(min(3, len(df))):
            row_text = " ".join(map(str, df.iloc[row_idx].tolist())).lower()
            if "firm" in row_text and "revenue" in row_text:
                header_table_idx = (i, row_idx)
                break
        if header_table_idx:
            break
    
    # Combine all tables into one master table
    combined_rows = []
    
    for i, df in enumerate(enr_tables):
        if header_table_idx and i == header_table_idx[0]:
            # This is the table with the header
            header_row_idx = header_table_idx[1]
            # Skip the header, add all data rows
            combined_rows.extend(df.iloc[header_row_idx + 1:].values.tolist())
            # Store the header for column names
            header_row = df.iloc[header_row_idx].tolist()
        else:
            # Regular data rows, add all rows (they might have a repeated header we need to skip)
            for row in df.values.tolist():
                # Check if this row looks like a header
                row_str = " ".join(map(str, row)).lower()
                if "firm" in row_str and "revenue" in row_str:
                    continue  # Skip header rows
                combined_rows.append(row)
    
    if not combined_rows:
        raise RuntimeError("No data rows found in tables.")
    
    # Create DataFrame from combined rows
    master_df = pd.DataFrame(combined_rows)
    
    # Use header if we found it, otherwise infer from first row
    if header_table_idx:
        # Limit header to number of columns in master_df
        master_df.columns = header_row[:len(master_df.columns)]
    else:
        # Try to find header in first row
        first_row_text = " ".join(map(str, master_df.iloc[0].tolist())).lower()
        if "firm" in first_row_text or "revenue" in first_row_text:
            master_df.columns = master_df.iloc[0]
            master_df = master_df.iloc[1:].reset_index(drop=True)
    
    print(f"Master table shape: {master_df.shape}")
    
    # Normalize the master table
    normalized = normalize_enr_table(master_df)
    return enr_table_to_dicts(normalized)


# ---------- Run as script ----------
if __name__ == "__main__":
    import sys
    from tabulate import tabulate

    if len(sys.argv) < 2:
        print("Usage: python enr_parser_v2.py <path_to_enr_pdf>")
        sys.exit(1)

    pdf_path = sys.argv[1]
    records = parse_enr_pdf(pdf_path)
    df = pd.DataFrame(records)
    print(tabulate(df.head(10), headers="keys", tablefmt="psql"))
    df.to_csv("enr_parsed.csv", index=False)
    print(f"\n✅ Extracted {len(df)} rows and saved to enr_parsed.csv")
