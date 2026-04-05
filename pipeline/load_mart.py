from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_data_mart(engine) -> int:
    """
    Load aggregated data from DWH into MART table.

    Returns:
        int: Number of rows loaded into MART.

    Raises:
        Exception: If MART load fails.
    """
    dwh_table = settings.dwh_table
    mart_table = settings.mart_table

    logger.info(
        f"Starting MART load: source='{dwh_table}', target='{mart_table}'"
    )

    try:
        truncate_mart_table(engine, mart_table)
        insert_rows_to_mart(engine, dwh_table, mart_table)
        mart_rows = get_mart_row_count(engine, mart_table)

        logger.info(
            f"MART load finished: table='{mart_table}', rows={mart_rows}"
        )

        return mart_rows

    except Exception as e:
        logger.exception(f"MART load failed: {e}")
        raise


def truncate_mart_table(engine, table_name: str) -> None:
    """
    Truncate MART table before reload.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))

    logger.info(f"MART table truncated: '{table_name}'")


def insert_rows_to_mart(engine, dwh_table: str, mart_table: str) -> None:
    """
    Aggregate data from DWH and insert into MART table.
    """
    insert_sql = f"""
        INSERT INTO {mart_table} (
            order_date,
            total_orders,
            total_quantity,
            total_revenue
        )
        SELECT
            DATE(invoicedate) AS order_date,
            COUNT(DISTINCT invoiceno) AS total_orders,
            SUM(quantity) AS total_quantity,
            SUM(revenue) AS total_revenue
        FROM {dwh_table}
        GROUP BY DATE(invoicedate)
        ORDER BY order_date;
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))

    logger.info(f"Data inserted into MART table '{mart_table}'")


def get_mart_row_count(engine, table_name: str) -> int:
    """
    Get number of rows in MART table.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    return result