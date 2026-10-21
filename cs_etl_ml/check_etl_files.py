
from pathlib import Path

# Main project folder
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# Files used in our project
files_to_check = [
    DATA_DIR / "online_retail_II.csv",
    DATA_DIR / "online_retail_II_customer_ids_filled.csv",
    DATA_DIR / "cleaned_retail.csv",
]

print("\n--- ETL DATA FILE CHECK ---")

for file_path in files_to_check:
    if file_path.exists():
        print(f"FOUND: {file_path.name} ({file_path.stat().st_size:,} bytes)")
    else:
        print(f"NOT FOUND: {file_path.name}")

print("\nCheck completed. No files were modified.")
