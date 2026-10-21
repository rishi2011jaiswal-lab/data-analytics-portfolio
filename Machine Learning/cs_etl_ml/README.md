# CS: ETL Pipeline with Machine Learning Integration

## Project Overview

This project demonstrates an automated ETL pipeline built using Apache Airflow, Python, PostgreSQL, Docker, and machine learning.

The pipeline is scheduled to run every 10 minutes.

## Project Workflow

1. **Extract:** Read retail transaction data.
2. **Transform:** Clean the data and prepare it for analysis.
3. **Load:** Store the cleaned data in PostgreSQL.
4. **Machine Learning:** Generate quantity predictions using a trained model.
5. **Store Predictions:** Save prediction results in the database.

## Technologies Used

- Python
- Apache Airflow
- PostgreSQL
- Docker and Docker Compose
- Pandas
- Scikit-learn

## Project Structure

```text
cs_etl_ml/
├── dags/
│   └── etl_pipeline.py
├── data/
├── models/
│   ├── train_model.py
│   └── retail_quantity_model.joblib
├── src/
│   ├── etl.py
│   ├── inspect_data.py
│   └── predict.py
├── tests/
├── Dockerfile.airflow
├── docker-compose.yaml
└── README.md
```

## Dataset

The retail CSV datasets are excluded from this repository because of their large size.

Place the required input dataset in the `data/` directory before running the pipeline. Check the ETL script for the exact expected filename and columns.

## Configuration and Security

Create a local `.env` file containing the `POSTGRES_PASSWORD` variable before starting the project. Do not commit `.env` or real credentials to GitHub.

## How to Run

1. Install Docker Desktop and ensure Docker is running.
2. Create the local `.env` file.
3. Place the required dataset in the `data/` directory.
4. Open PowerShell in the `cs_etl_ml` directory.
5. Validate the Compose configuration:

   ```powershell
   docker compose config -q
   ```

6. Start the services:

   ```powershell
   docker compose up -d --build
   ```

7. Open the Airflow interface at `http://localhost:8080` and check the DAG runs.

## Notes

This is a learning and portfolio project. Review the configuration and replace local placeholder credentials before using it outside a development environment.
