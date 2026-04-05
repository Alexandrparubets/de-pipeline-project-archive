from pipeline.logger_config import get_logger, set_run_id
from pipeline.connection import get_engine, test_connection
from pipeline.setup_db import setup_database
from pipeline.metadata import (start_pipeline_run,
    finish_pipeline_run_success,
    finish_pipeline_run_failed,
    get_last_successful_watermark
)
from pipeline.extract import get_source_file_path
from pipeline.raw import create_raw_copy
from pipeline.transform import load_raw_to_dataframe, clean_dataframe
from pipeline.load_stg import load_to_stg, align_to_stg_columns
from pipeline.quality import run_quality_checks


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
        last_watermark = get_last_successful_watermark(engine, pipeline_name)
        source_file = get_source_file_path()
        raw_file_path, file_hash = create_raw_copy(source_file, pipeline_name)
        df = load_raw_to_dataframe(raw_file_path, last_loaded_date = last_watermark)
        df, watermark_value = clean_dataframe(df)
        df = align_to_stg_columns(df)
        rows_in_stg = load_to_stg(df, engine)
        run_quality_checks(engine)
        

        finish_pipeline_run_success(
            engine=engine,
            run_id=run_id,
            rows_in_stg=rows_in_stg,
            watermark_value=watermark_value,
        )
        logger.info("✅ Pipeline finished")

    except Exception as e:
        finish_pipeline_run_failed(
            engine=engine,
            run_id=run_id,
            error_message=str(e),
        )
        raise

if __name__ == "__main__":
    run_pipeline()