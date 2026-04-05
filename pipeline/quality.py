import logging

from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def run_quality_checks(engine) -> bool:
    """
    Run data quality checks against the STG table.

    Args:
        engine: SQLAlchemy engine for database connection.

    Returns:
        bool: True if all quality checks passed.

    Raises:
        ValueError: If one or more quality checks failed.
    """
    table_name = settings.stg_table
    errors = []

    logger.info(f"Starting quality checks for STG table '{table_name}'")

    errors.extend(check_required_fields(engine, table_name))
    errors.extend(check_duplicates(engine, table_name))
    errors.extend(check_value_ranges(engine, table_name))
    

    if errors:
        logger.error("Quality checks failed.")
        for error in errors:
            logger.error(f"QUALITY ERROR: {error}")
        raise ValueError("STG quality checks failed")

    logger.info("All STG quality checks passed successfully.")
    return True


def check_required_fields(engine, table_name: str) -> list[str]:
    """
    Check for NULL values in required columns.

    Returns:
        list[str]: List of error messages.
    """
    required_columns = [
        "invoiceno",
        "stockcode",
        "quantity",
        "invoicedate",
        "unitprice",
        "customerid",
        "country",
        "revenue",
        "row_hash",
    ]

    errors = []

    with engine.begin() as conn:
        for column in required_columns:
            result = conn.execute(
                text(f"""
                    SELECT COUNT(*) 
                    FROM {table_name}
                    WHERE {column} IS NULL
                """)
            ).scalar()

            if result > 0:
                errors.append(f"NULL values found in column '{column}': {result}")
                logger.warning(
                    f"Check NULLs FAILED for '{column}': {result} rows"
                )
            else:
                logger.info(f"Check NULLs OK for '{column}'")

    return errors


def check_duplicates(engine, table_name: str) -> list[str]:
    """
    Check for duplicate rows based on row_hash.

    Returns:
        list[str]: List of error messages.
    """
    errors = []

    with engine.begin() as conn:
        result = conn.execute(
            text(f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT row_hash
                    FROM {table_name}
                    GROUP BY row_hash
                    HAVING COUNT(*) > 1
                ) t
            """)
        ).scalar()

        if result > 0:
            errors.append(f"Duplicate row_hash values found: {result}")
            logger.warning(f"Check duplicates FAILED: {result} duplicate groups")
        else:
            logger.info("Check duplicates OK (no duplicates found)")

    return errors


def check_value_ranges(engine, table_name: str) -> list[str]:
    """
    Check business rules for numeric fields.

    Returns:
        list[str]: List of error messages.
    """
    errors = []

    checks = [
        ("quantity <= 0", "Quantity must be > 0"),
        ("revenue <= 0", "Revenue must be > 0"),
        ("unitprice < 0", "UnitPrice must be >= 0"),
    ]

    with engine.begin() as conn:
        for condition, description in checks:
            result = conn.execute(
                text(f"""
                    SELECT COUNT(*) 
                    FROM {table_name}
                    WHERE {condition}
                """)
            ).scalar()

            if result > 0:
                errors.append(f"{description}: {result} rows")
                logger.warning(f"Check FAILED: {description} ({result} rows)")
            else:
                logger.info(f"Check OK: {description}")

    return errors