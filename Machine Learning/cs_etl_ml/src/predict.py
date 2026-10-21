
import logging
import os
from pathlib import Path

import joblib
import pandas as pd
from sqlalchemy import create_engine, text


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000
MODEL_VERSION = "retail_quantity_model_v1"

FEATURE_COLUMNS = [
    "price",
    "stock_code",
    "country",
    "month",
    "day_of_week",
    "hour",
]


def run_predictions():
    logger.info("Starting retail quantity predictions")

    database_url = os.environ.get("APP_DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "APP_DATABASE_URL environment variable is missing"
        )

    project_dir = Path("/opt/airflow")
    model_path = project_dir / "models" / "retail_quantity_model.joblib"

    if not model_path.is_file():
        raise FileNotFoundError(
            f"Trained model not found: {model_path}"
        )

    model = joblib.load(model_path)
    engine = create_engine(database_url)

    total_selected = 0
    total_inserted = 0
    last_id = 0

    try:
        # Ensure the expected columns and source-ID index exist.
        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE retail_predictions
                ADD COLUMN IF NOT EXISTS source_id BIGINT
            """))

            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS
                    retail_predictions_source_id_uidx
                ON retail_predictions (source_id)
                WHERE source_id IS NOT NULL
            """))

        while True:
            # Select unpredicted transactions after the last processed ID.
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT
                        r.id AS source_id,
                        r.invoice,
                        r.stock_code,
                        r.invoice_date,
                        r.quantity AS actual_quantity,
                        r.price,
                        r.country,
                        EXTRACT(
                            MONTH FROM r.invoice_date
                        )::INTEGER AS month,
                        EXTRACT(
                            DOW FROM r.invoice_date
                        )::INTEGER AS day_of_week,
                        EXTRACT(
                            HOUR FROM r.invoice_date
                        )::INTEGER AS hour
                    FROM cleaned_retail AS r
                    WHERE r.id > :last_id
                      AND r.quantity > 0
                      AND r.price > 0
                      AND r.invoice_date IS NOT NULL
                      AND r.stock_code IS NOT NULL
                      AND r.country IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM retail_predictions AS p
                          WHERE p.source_id = r.id
                             OR (
                                 p.invoice = r.invoice
                                 AND p.stock_code = r.stock_code
                                 AND p.invoice_date = r.invoice_date
                             )
                      )
                    ORDER BY r.id
                    LIMIT :batch_size
                """), {
                    "last_id": last_id,
                    "batch_size": BATCH_SIZE,
                }).mappings().all()

            if not rows:
                break

            source = pd.DataFrame([dict(row) for row in rows])
            batch_last_id = int(source["source_id"].max())
            total_selected += len(source)

            logger.info(
                "Selected %s transactions; IDs %s to %s",
                len(source),
                int(source["source_id"].min()),
                batch_last_id,
            )

            # Prepare the exact feature columns used during model training.
            features = source[FEATURE_COLUMNS].copy()

            features["price"] = pd.to_numeric(
                features["price"], errors="coerce"
            )

            for column in ["stock_code", "country"]:
                features[column] = (
                    features[column]
                    .astype("string")
                    .fillna("Unknown")
                )

            if features.isnull().any().any():
                raise ValueError(
                    "Prediction batch contains missing model features"
                )

            predictions = model.predict(features)

            output = pd.DataFrame({
                "source_id": source["source_id"].astype("int64"),
                "invoice": source["invoice"].astype(str),
                "stock_code": source["stock_code"].astype(str),
                "invoice_date": source["invoice_date"],
                "actual_quantity": source["actual_quantity"].astype(int),
                "predicted_quantity": predictions,
                "model_version": MODEL_VERSION,
            })

            records = output.to_dict(orient="records")

            # ON CONFLICT DO NOTHING handles ALL unique constraints,
            # including the existing invoice/stock_code/invoice_date key.
            with engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO retail_predictions (
                        source_id,
                        invoice,
                        stock_code,
                        invoice_date,
                        actual_quantity,
                        predicted_quantity,
                        model_version
                    )
                    VALUES (
                        :source_id,
                        :invoice,
                        :stock_code,
                        :invoice_date,
                        :actual_quantity,
                        :predicted_quantity,
                        :model_version
                    )
                    ON CONFLICT DO NOTHING
                """), records)

            inserted = max(result.rowcount, 0)
            total_inserted += inserted

            # Always advance, even if every row conflicted.
            last_id = batch_last_id

            logger.info(
                "Batch complete: selected=%s inserted=%s last_id=%s",
                len(source),
                inserted,
                last_id,
            )

        with engine.connect() as conn:
            total_predictions = conn.execute(text("""
                SELECT COUNT(*) FROM retail_predictions
            """)).scalar_one()

        logger.info("Total candidates processed: %s", total_selected)
        logger.info("New predictions inserted: %s", total_inserted)
        logger.info("Total stored predictions: %s", total_predictions)
        logger.info("Prediction pipeline completed successfully")

    finally:
        engine.dispose()


if __name__ == "__main__":
    run_predictions()
