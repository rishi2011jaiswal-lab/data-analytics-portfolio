
from pathlib import Path
import os

import joblib
import pandas as pd

from sqlalchemy import create_engine, text

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


def train_model():
    print("Starting retail quantity model training...", flush=True)

    database_url = os.environ.get("APP_DATABASE_URL")
    if not database_url:
        raise RuntimeError("APP_DATABASE_URL environment variable is missing.")

    engine = create_engine(database_url)

    query = """
        SELECT
            quantity,
            price,
            stock_code,
            country,
            EXTRACT(MONTH FROM invoice_date)::INTEGER AS month,
            EXTRACT(DOW FROM invoice_date)::INTEGER AS day_of_week,
            EXTRACT(HOUR FROM invoice_date)::INTEGER AS hour
        FROM cleaned_retail
        WHERE quantity > 0
          AND price > 0
          AND invoice_date IS NOT NULL
          AND stock_code IS NOT NULL
          AND country IS NOT NULL
        ORDER BY invoice_date
        LIMIT 200000
    """

    
   
    try:
        query = """
            SELECT
                quantity,
                price,
                stock_code,
                country,
                EXTRACT(MONTH FROM invoice_date)::INTEGER AS month,
                EXTRACT(DOW FROM invoice_date)::INTEGER AS day_of_week,
                EXTRACT(HOUR FROM invoice_date)::INTEGER AS hour
            FROM cleaned_retail
            WHERE quantity > 0
              AND price > 0
              AND invoice_date IS NOT NULL
              AND stock_code IS NOT NULL
              AND country IS NOT NULL
            ORDER BY invoice_date
            LIMIT 200000
        """

        with engine.connect() as connection:
            result = connection.exec_driver_sql(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

    finally:
        engine.dispose()



    if len(df) < 100:
        raise ValueError(f"Not enough valid records to train: {len(df)}")

    print(f"Training records loaded: {len(df)}", flush=True)

    features = [
        "price",
        "stock_code",
        "country",
        "month",
        "day_of_week",
        "hour",
    ]

    X = df[features].copy()
    y = df["quantity"].astype("int64")

    split_index = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    numeric_features = ["price", "month", "day_of_week", "hour"]
    categorical_features = ["stock_code", "country"]

    preprocessing = ColumnTransformer(
        transformers=[
            (
                "numeric",
                SimpleImputer(strategy="median"),
                numeric_features,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "onehot",
                            OneHotEncoder(
                                handle_unknown="ignore",
                                min_frequency=5,
                            ),
                        ),
                    ]
                ),
                categorical_features,
            ),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessing", preprocessing),
            (
                "regressor",
                RandomForestRegressor(
                    n_estimators=100,
                    max_depth=12,
                    min_samples_leaf=2,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    print("Training Random Forest model...", flush=True)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)
    rmse = mean_squared_error(y_test, predictions) ** 0.5

    print(f"Evaluation MAE: {mae:.3f}", flush=True)
    print(f"Evaluation RMSE: {rmse:.3f}", flush=True)

    model_dir = Path("/opt/airflow/models")
    model_dir.mkdir(parents=True, exist_ok=True)

    model_path = model_dir / "retail_quantity_model.joblib"
    joblib.dump(model, model_path)

    print(f"Model saved to: {model_path}", flush=True)
    print("Model training completed successfully.", flush=True)


if __name__ == "__main__":
    train_model()
