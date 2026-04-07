from pipeline.logger_config import get_logger, set_run_id
from pipeline.connection import get_engine, test_connection
from pipeline.setup_db import setup_database
from pipeline.metadata import (start_pipeline_run,
    finish_pipeline_run_success,
    finish_pipeline_run_failed,
    get_last_successful_watermark,
    )
from pipeline.extract import get_source_file_path
from pipeline.raw import create_raw_copy
from pipeline.transform import load_raw_to_dataframe, clean_dataframe, calculate_historical_hash
from pipeline.load_stg import load_to_stg, align_to_stg_columns
from pipeline.quality import run_quality_checks
from pipeline.load_dwh import load_stg_to_dwh
from pipeline.load_mart import load_data_mart


logger = get_logger("pipeline.run")
pipeline_name = "NEW PIPELINE"

def run_pipeline() -> None:
   
    logger.info("🚀 Pipeline process started")

    try:

        
        engine = get_engine()
        test_connection(engine)
        setup_database(engine)
        run_id = start_pipeline_run(engine, pipeline_name)
        set_run_id(run_id)
        last_watermark, boundary_date = get_last_successful_watermark(engine, pipeline_name)
        source_file = get_source_file_path()
        raw_file_path, file_hash = create_raw_copy(source_file, pipeline_name)
        df, new_historical_hash, new_boundary_date = load_raw_to_dataframe(engine, pipeline_name, raw_file_path, last_watermark, boundary_date)

       

        if df.empty:
            finish_pipeline_run_success(
                engine=engine,
                run_id=run_id,
                rows_in_stg=0,
                watermark_value=last_watermark,
                boundary_date = new_boundary_date,
                historical_hash=new_historical_hash,
                rows_loaded_to_dwh=0,
                rows_skipped_in_dwh=0,
            )
            return

        df, watermark_value = clean_dataframe(df)
        df = align_to_stg_columns(df)
        rows_in_stg = load_to_stg(df, engine)
        run_quality_checks(engine)
        dwh_stats = load_stg_to_dwh(engine)
        attempted_rows = dwh_stats["attempted_rows"]
        inserted_rows = dwh_stats["inserted_rows"]
        skipped_rows = dwh_stats["skipped_rows"]
        mart_rows = load_data_mart(engine)
        

        finish_pipeline_run_success(
            engine=engine,
            run_id=run_id,
            rows_in_stg=rows_in_stg,
            watermark_value=watermark_value,
            boundary_date = new_boundary_date,
            historical_hash=new_historical_hash,
            rows_loaded_to_dwh=inserted_rows,
            rows_skipped_in_dwh=skipped_rows,
        )
        logger.info("✅ Pipeline finished")

    except ValueError as e:
        finish_pipeline_run_failed(
            engine=engine,
            run_id=run_id,
            error_message=str(e),
        )
        logger.error(f"Pipeline stopped due to validation error: {e}")
        return    

    except Exception as e:
        finish_pipeline_run_failed(
            engine=engine,
            run_id=run_id,
            error_message=str(e),
        )
        raise

if __name__ == "__main__":
    run_pipeline()