import os
import sys
import time
import sqlite3
import pandas as pd
from tkinter import Tk, filedialog

# ============================================================
# File picker
# ============================================================
def pick_file(title, filetypes):
    root = Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(title=title, filetypes=filetypes)
    root.destroy()
    return path

# ============================================================
# SQLite helpers
# ============================================================
def get_table_with_column(conn, col_name):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    for (table,) in cur.fetchall():
        cur.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cur.fetchall()]
        if col_name in cols:
            return table
    raise RuntimeError(f"No table contains column '{col_name}'.")

def table_has_column(conn, table, col_name):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == col_name for row in cur.fetchall())

def count_accepted(conn, table, phrase):
    """
    Count rows where:
      - search_term matches phrase (case-insensitive, trimmed)
      - corr_def is exactly 1.0 (numeric)
      - not_bot is 1
    """
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE LOWER(TRIM(search_term)) = LOWER(TRIM(?))
          AND CAST(corr_def AS REAL) = 1.0
          AND CAST(not_bot AS INTEGER) = 1
        """,
        (phrase,),
    )
    return int(cur.fetchone()[0])

# ============================================================
# Main
# ============================================================
def main():
    db_path = pick_file(
        "Select SQLite Database (.db)",
        [("SQLite Database", "*.db"), ("All files", "*.*")]
    )
    if not db_path:
        print("No database selected. Exiting.")
        return

    csv_path = pick_file(
        "Select CSV File (.csv)",
        [("CSV files", "*.csv"), ("All files", "*.*")]
    )
    if not csv_path:
        print("No CSV selected. Exiting.")
        return

    # Load CSV
    df = pd.read_csv(csv_path)

    # Strip whitespace from column headers to avoid header mismatch bugs
    df.columns = [c.strip() for c in df.columns]

    if "phrase" not in df.columns:
        raise RuntimeError("CSV must contain a 'phrase' column.")

    # Ensure output column exists (your CSV should already have it, but this prevents blanks/errors)
    if "final_ammount" not in df.columns:
        df["final_ammount"] = 0

    # Make sure it's numeric (so Excel shows 0 instead of blank)
    df["final_ammount"] = 0

    # Connect DB and find table
    conn = sqlite3.connect(db_path)
    try:
        table = get_table_with_column(conn, "search_term")
        print(f"Using table: {table}")

        if not table_has_column(conn, table, "corr_def"):
            raise RuntimeError(f"Database table '{table}' is missing column 'corr_def'.")
        if not table_has_column(conn, table, "not_bot"):
            raise RuntimeError(f"Database table '{table}' is missing column 'not_bot'.")

        # Fill final_ammount for each phrase
        filled = 0
        for i, raw_phrase in enumerate(df["phrase"]):
            if pd.isna(raw_phrase):
                continue
            phrase = str(raw_phrase).strip()
            if not phrase:
                continue

            df.at[i, "final_ammount"] = count_accepted(conn, table, phrase)
            filled += 1

        print(f"Filled final_ammount for {filled} phrase rows.")

        # Save NEW Excel file in same folder as selected CSV
        output_dir = os.path.dirname(csv_path)
        base_output = os.path.join(output_dir, "not_bugged final tweet ammount.xlsx")

        try:
            df.to_excel(base_output, index=False)
            print(f"\nSaved Excel to:\n{base_output}")
        except PermissionError:
            # If the file is open/locked (Excel or OneDrive), save a timestamped copy
            ts = time.strftime("%Y%m%d_%H%M%S")
            fallback = os.path.join(output_dir, f"not_bugged final tweet ammount_{ts}.xlsx")
            df.to_excel(fallback, index=False)
            print(f"\nFile was locked. Saved instead to:\n{fallback}")

    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
