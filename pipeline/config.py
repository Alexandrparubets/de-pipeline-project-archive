from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    # DB
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", 5432))
    db_name: str = os.getenv("DB_NAME", "de_db")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")

    # Logging
    log_file: str = os.getenv("LOG_FILE", "logs/pipeline.log")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    stg_table: str = os.getenv("STG_TABLE", "stg_orders")
    dwh_table: str = os.getenv("DWH_TABLE", "orders_clean")
    mart_table: str = os.getenv("MART_TABLE", "sales_daily")
    pipeline_runs_table: str = os.getenv("PIPELINE_RUNS_TABLE", "pipeline_runs")

    source_file: str = os.getenv(
    "SOURCE_FILE",
    "data/source/online_retail.xlsx",
)


settings = Settings()