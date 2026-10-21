
from pathlib import Path
import os

import pandas as pd
from sqlalchemy import create_engine, text


def run_etl():
    # 1. Define project paths
    project_dir = Path("/opt/airflow")
    input_file = project_dir / "data" / "online_retail_II.csv"
    output_file = project_dir / "data" / "cleaned_retail.csv"

    print("Starting ETL pipeline...")

    # 2. Extract
    if not input_file.exists():
        raise FileNotFoundError(f"Source CSV not found: {input_file}")

    df = pd.read_csv(input_file)
    print(f"Rows extracted: {len(df)}")

    # 3. Validate source columns
    df.columns = df.columns.str.strip()

    required_columns = [
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Customer ID",
        "Country",
    ]

    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Missing source columns: {sorted(missing_columns)}"
        )

    df = df[required_columns].copy()

    # 4. Clean and transform values
    for column in ["Invoice", "StockCode"]:
        df[column] = df[column].astype("string").str.strip()

    df["Description"] = (
        df["Description"].astype("string").str.strip()
    )
    df["Country"] = df["Country"].astype("string").str.strip()

    df["InvoiceDate"] = pd.to_datetime(
        df["InvoiceDate"], errors="coerce"
    )
    df["Quantity"] = pd.to_numeric(
        df["Quantity"], errors="coerce"
    )
    df["Price"] = pd.to_numeric(
        df["Price"], errors="coerce"
    )
    df["Customer ID"] = pd.to_numeric(
        df["Customer ID"], errors="coerce"
    )



    # Fill missing Customer IDs by invoice group
    # Only generate an ID when every row in the invoice group is missing one.
    missing_invoice_groups = (
        df.groupby("Invoice", sort=False)["Customer ID"]
        .filter(lambda ids: ids.isna().all())
        .index
    )

    missing_invoices = (
        df.loc[missing_invoice_groups, "Invoice"]
        .drop_duplicates()
        .tolist()
    )

    for new_customer_id, invoice in enumerate(
        missing_invoices, start=18288
    ):
        df.loc[
            (df["Invoice"] == invoice) & df["Customer ID"].isna(),
            "Customer ID",
        ] = new_customer_id

    print(
        "Missing Customer IDs after grouping:",
        df["Customer ID"].isna().sum(),
    )



   
 

    # Remove exact duplicate source records
    before = len(df)
    df = df.drop_duplicates()
    print(f"Duplicate rows removed: {before - len(df)}")

    # Essential fields must be present
    df = df.dropna(
        subset=[
            "Invoice",
            "StockCode",
            "InvoiceDate",
            "Quantity",
            "Price",
            "Country",
        ]
    )

    # Retain valid sales transactions
    df = df[
        (df["Quantity"] > 0)
        & (df["Price"] > 0)
        & (df["Invoice"].str.len() > 0)
        & (df["StockCode"].str.len() > 0)
        & (df["Country"].str.len() > 0)
    ].copy()

    # Retail quantities should be whole numbers
    df = df[df["Quantity"] % 1 == 0].copy()
    df["Quantity"] = df["Quantity"].astype("int64")

    # Match PostgreSQL column names
    df = df.rename(
        columns={
            "Invoice": "invoice",
            "StockCode": "stock_code",
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_date",
            "Price": "price",
            "Customer ID": "customer_id",
            "Country": "country",
        }
    )

    # Convert pandas missing values to Python None for SQL NULL
    for column in ["description", "customer_id"]:
        df[column] = df[column].astype(object)
        df[column] = df[column].where(
            pd.notna(df[column]), None
        )

    # 5. Save the cleaned CSV
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)

    print(f"Rows after cleaning: {len(df)}")
    print(f"Cleaned CSV saved to: {output_file}")

    # 6. Connect to PostgreSQL
    db_url = os.environ.get("APP_DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "APP_DATABASE_URL environment variable is missing"
        )

    engine = create_engine(db_url)


    


    try:
        with engine.begin() as conn:
            # Create the destination table if it does not exist
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS cleaned_retail (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    invoice VARCHAR(20) NOT NULL,
                    stock_code VARCHAR(30) NOT NULL,
                    description TEXT,
                    quantity INTEGER NOT NULL,
                    invoice_date TIMESTAMP NOT NULL,
                    price NUMERIC(12,2) NOT NULL,
                    customer_id NUMERIC(12,1),
                    country VARCHAR(100) NOT NULL,
                    loaded_at TIMESTAMP NOT NULL
                        DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # Support missing customer IDs in existing tables
            conn.execute(text("""
                ALTER TABLE cleaned_retail
                ALTER COLUMN customer_id DROP NOT NULL
            """))

            # Ensure the idempotency key exists.
            # If old duplicate database rows exist, index creation
            # will fail rather than silently deleting historical data.
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS
                    cleaned_retail_transaction_uidx
                ON cleaned_retail
                    (invoice, stock_code, invoice_date, quantity, price)
            """))

            # A temporary staging table with no identity/PK constraints
            conn.execute(text("""
                CREATE TEMP TABLE cleaned_retail_stage
                ON COMMIT DROP AS
                SELECT
                    invoice,
                    stock_code,
                    description,
                    quantity,
                    invoice_date,
                    price,
                    customer_id,
                    country
                FROM cleaned_retail
                WITH NO DATA
            """))

            columns = [
                "invoice",
                "stock_code",
                "description",
                "quantity",
                "invoice_date",
                "price",
                "customer_id",
                "country",
            ]

            # Insert in batches to avoid creating one huge Python list
            insert_stage = text("""
                INSERT INTO cleaned_retail_stage (
                    invoice, stock_code, description, quantity,
                    invoice_date, price, customer_id, country
                )
                VALUES (
                    :invoice, :stock_code, :description, :quantity,
                    :invoice_date, :price, :customer_id, :country
                )
            """)

            batch_size = 5000
            staged_rows = 0

            for start in range(0, len(df), batch_size):
                batch_df = df.iloc[start:start + batch_size][columns]
                records = batch_df.to_dict(orient="records")

                if records:
                    conn.execute(insert_stage, records)
                    staged_rows += len(records)

                print(
                    f"Staged {staged_rows} of {len(df)} rows"
                )

            # Insert new transactions; skip previously loaded records
            result = conn.execute(text("""
                INSERT INTO cleaned_retail (
                    invoice, stock_code, description, quantity,
                    invoice_date, price, customer_id, country
                )
                SELECT
                    invoice, stock_code, description, quantity,
                    invoice_date, price, customer_id, country
                FROM cleaned_retail_stage
                ON CONFLICT (
                    invoice, stock_code, invoice_date, quantity, price
                )
                DO NOTHING
            """))

            print(f"New database rows inserted: {result.rowcount}")

            total = conn.execute(text("""
                SELECT COUNT(*) FROM cleaned_retail
            """)).scalar_one()

        print(f"Total rows in PostgreSQL cleaned_retail: {total}")
        print("ETL pipeline completed successfully.")

    finally:
        engine.dispose()


if __name__ == "__main__":
    run_etl()
