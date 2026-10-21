
from pathlib import Path

import pandas as pd


def inspect_dataset() -> None:
    """Inspect the Online Retail II CSV and save a data-quality report."""

    project_dir = Path(__file__).resolve().parent.parent
    csv_path = project_dir / "data" / "online_retail_II.csv"
    report_path = project_dir / "data" / "data_quality_report.txt"

    if not csv_path.exists():
        raise FileNotFoundError(f"Dataset not found: {csv_path}")

    print("Reading dataset...")
    df = pd.read_csv(
        csv_path,
        encoding="ISO-8859-1",
        low_memory=False,
    )

    report = [
        "ONLINE RETAIL II — DATA QUALITY REPORT",
        "=" * 45,
        f"Rows: {len(df):,}",
        f"Columns: {len(df.columns)}",
        "",
        "COLUMN NAMES",
        "-" * 45,
        *df.columns.tolist(),
        "",
        "DATA TYPES",
        "-" * 45,
        df.dtypes.to_string(),
        "",
        "MISSING VALUES",
        "-" * 45,
        df.isnull().sum().to_string(),
        "",
        f"Exact duplicate rows: {df.duplicated().sum():,}",
        "",
        "FIRST FIVE ROWS",
        "-" * 45,
        df.head().to_string(index=False),
    ]

    report_path.write_text("\n".join(report), encoding="utf-8")

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Report saved to: {report_path}")


if __name__ == "__main__":
    inspect_dataset()
