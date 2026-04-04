from pipeline.logger_config import get_logger, set_run_id
from pipeline.connection import get_engine, test_connection
from pipeline.setup_db import setup_database
from pipeline.metadata import (start_pipeline_run,
    finish_pipeline_run_success,
    finish_pipeline_run_failed
)


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
        
        finish_pipeline_run_success(
            engine=engine,
            run_id=run_id,
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