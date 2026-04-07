import pandas as pd
from pathlib import Path
import hashlib

from pipeline.logger_config import get_logger
from pipeline.metadata import get_last_successful_historical_hash
from pipeline.setup_db import truncate_dwh_table


logger = get_logger("pipeline.transform")


def load_raw_to_dataframe(engine, pipeline_name, raw_file_path: Path, last_watermark=None, boundary_date=None ) -> pd.DataFrame:
    """
    Reads RAW Excel file into pandas DataFrame,
    validates that it is not empty,
    normalizes column names,
    and logs load statistics.
    """
    logger.info(f"Starting RAW load into DataFrame: '{raw_file_path}'")

    try:
        df = pd.read_excel(raw_file_path)
    except Exception as e:
        logger.error(f"Failed to read RAW file: '{raw_file_path}' | error: {e}")
        raise

    if df.empty:
        logger.error(f"RAW file is empty: '{raw_file_path}'")
        raise ValueError(f"RAW file is empty: {raw_file_path}")
    
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df.columns = normalize_column_names(df.columns)
    
    last_historical_hash = get_last_successful_historical_hash(engine, pipeline_name)
    logger.info(f"last_historical_hash: {last_historical_hash}")
    historical_hash, new_boundary_date, new_historical_hash = calculate_historical_hash(df, boundary_date)
    logger.info(f"historical_hash: {historical_hash}")
    logger.info(f"new_historical_hash: {new_historical_hash}")

    if (historical_hash is not None
        and historical_hash != last_historical_hash
    ):
        truncate_dwh_table(engine)
        last_watermark = None
        logger.warning(
            "Historical source data has changed compared to the last successful run."
        )
   
    logger.info(f"Initial rows loaded from RAW: {len(df)}")

    if last_watermark is None:
        logger.info("No watermark found. Full RAW file will be loaded.")
    else:
        logger.info(f"Applying watermark filter: InvoiceDate >= {last_watermark}")
        df = df[df["invoicedate"] >= last_watermark].copy()
        logger.info(f"Rows after watermark filter: {len(df)}")
        if df.empty:
            logger.info("No new rows found after watermark filter.")
            return df, new_historical_hash, new_boundary_date

    

    logger.info(
        f"RAW file loaded successfully: '{raw_file_path}' | "
        f"rows={df.shape[0]}, columns={df.shape[1]}"
    )
    logger.info(f"Normalized columns: {list(df.columns)}")

    return df, new_historical_hash, new_boundary_date


def normalize_column_names(columns) -> list[str]:
    """
    Normalizes column names:
    - strips spaces
    - converts to lowercase
    - replaces spaces with underscores
    """
    normalized = []
    for col in columns:
        col = str(col).strip().lower().replace(" ", "_")
        normalized.append(col)

    return normalized


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and prepares DataFrame for next pipeline stages.
    """
    logger.info("Starting DataFrame cleaning")

    df = df.copy()

    initial_rows = len(df)
    logger.info(f"Initial rows before cleaning: {initial_rows}")

    df = remove_cancelled_orders(df)
    df = remove_non_positive_quantity(df)
    df = drop_missing_customer_id(df)
    df = convert_data_types(df)
    df = normalize_text_columns(df)
    df = create_revenue_column(df)
    df = remove_non_positive_revenue(df)

    if df.empty:
        logger.error("DataFrame became empty after cleaning")
        raise ValueError("DataFrame became empty after cleaning")

    df = create_row_hash(df)
    df = remove_duplicates_by_row_hash(df)

    final_rows = len(df)
    logger.info(
        f"DataFrame cleaning finished: rows_before={initial_rows}, rows_after={final_rows}"
    )

    watermark_value =  df["invoicedate"].max()
    logger.info(f"New watermark calculated: {watermark_value}")

    return df, watermark_value


def remove_cancelled_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes cancelled orders: InvoiceNo starting with 'C'
    """
    before = len(df)
    df = df[~df["invoiceno"].astype(str).str.upper().str.startswith("C", na=False)].copy()
    after = len(df)

    logger.info(f"Removed cancelled orders: {before - after} rows")
    return df


def remove_non_positive_quantity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows where quantity <= 0
    """
    before = len(df)
    df = df[df["quantity"] > 0].copy()
    after = len(df)

    logger.info(f"Removed non-positive quantity rows: {before - after} rows")
    return df


def drop_missing_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows with missing customerid
    """
    before = len(df)
    df = df.dropna(subset=["customerid"]).copy()
    after = len(df)

    logger.info(f"Removed rows with missing customerid: {before - after} rows")
    return df


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns to required data types
    """
    try:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="raise").astype(int)
        df["unitprice"] = pd.to_numeric(df["unitprice"], errors="raise")
        df["customerid"] = pd.to_numeric(df["customerid"], errors="raise").astype(int)
        df["invoicedate"] = pd.to_datetime(df["invoicedate"], errors="raise")
    except Exception as e:
        logger.error(f"Failed to convert data types: {e}")
        raise

    logger.info("Data types converted successfully")
    return df


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes text columns
    """
    text_columns = ["invoiceno", "stockcode", "description", "country"]

    for col in text_columns:
        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.upper()
        )

    logger.info(f"Normalized text columns: {text_columns}")
    return df


def create_revenue_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates revenue column = quantity * unitprice
    """
    df["revenue"] = (df["quantity"] * df["unitprice"]).round(2)

    logger.info("Created calculated column: revenue")
    return df


def remove_non_positive_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows where revenue <= 0
    """
    before = len(df)
    df = df[df["revenue"] > 0].copy()
    after = len(df)

    logger.info(f"Removed non-positive revenue rows: {before - after} rows")
    return df


def create_row_hash(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates technical row hash for deduplication
    """
    df["row_hash"] = df.apply(build_row_hash, axis=1)

    logger.info("Created technical key: row_hash")
    return df


def build_row_hash(row: pd.Series) -> str:
    """
    Builds SHA256 hash from normalized business fields
    """
    raw_string = "|".join(
        [
            str(row["invoiceno"]).strip().upper(),
            str(row["stockcode"]).strip().upper(),
            str(row["description"]).strip().upper(),
            str(int(row["quantity"])),
            row["invoicedate"].floor("min").strftime("%Y-%m-%d %H:%M"),
            f"{row['unitprice']:.4f}",
            str(int(row["customerid"])),
            str(row["country"]).strip().upper(),
        ]
    )

    return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()


def remove_duplicates_by_row_hash(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate rows by row_hash
    """
    before = len(df)
    df = df.drop_duplicates(subset=["row_hash"]).copy()
    after = len(df)

    logger.info(f"Removed duplicate rows by row_hash: {before - after} rows")
    return df


def calculate_historical_hash(df: pd.DataFrame, boundary_date) -> str | None:
    """
    Calculate hash for historical part of source data
    (rows with InvoiceDate < last_watermark).
    """

    logger.info(f"historical_hash -> boundary_date: {boundary_date} max data: {df["invoicedate"].max()}")
    if boundary_date is None:
        boundary_date = df["invoicedate"].max()
    
    historical_df = df[df["invoicedate"] < boundary_date].copy()
    
    new_boundary_date = df["invoicedate"].max()
    new_historical_df = df[df["invoicedate"] < new_boundary_date].copy()

    logger.info(f"historical_hash -> new boundary_date: {new_boundary_date}")

    if historical_df.empty:
        return None

    historical_df = historical_df.sort_values(
        by=list(historical_df.columns)
    ).fillna("")

    new_historical_df = new_historical_df.sort_values(
        by=list(new_historical_df.columns)
    ).fillna("")

    payload = historical_df.to_csv(index=False)
    historical_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    new_payload = new_historical_df.to_csv(index=False)
    new_historical_hash = hashlib.sha256(new_payload.encode("utf-8")).hexdigest()

    return historical_hash, new_boundary_date, new_historical_hash