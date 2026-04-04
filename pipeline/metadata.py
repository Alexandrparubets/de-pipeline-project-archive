from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.config import settings
from pipeline.logger_config import get_logger


logger = get_logger("pipeline.metadata")


def start_pipeline_run(engine: Engine, pipeline_name: str) -> int:
    """
    Creates a new record in pipeline_runs table
    and returns generated run_id.
    """
    insert_sql = f"""
    INSERT INTO {settings.pipeline_runs_table} (
        pipeline_name,
        status,
        started_at
    )
    VALUES (
        :pipeline_name,
        'running',
        :started_at
    )
    RETURNING id;
    """

    with engine.begin() as conn:
        run_id = conn.execute(
            text(insert_sql),
            {
                "pipeline_name": pipeline_name,
                "started_at": datetime.now(),
            },
        ).scalar()

    logger.info(
        f"Pipeline run started: id={run_id}, pipeline='{pipeline_name}', status='running'"
    )

    return run_id


def finish_pipeline_run_success(
    engine: Engine,
    run_id: int,
    watermark_value=None,
    rows_in_stg: int | None = None,
    rows_loaded_to_dwh: int | None = None,
    rows_skipped_in_dwh: int | None = None,
) -> None:
    """
    Updates pipeline run record with success status
    and final execution statistics.
    """
    update_sql = f"""
    UPDATE {settings.pipeline_runs_table}
    SET
        status = 'success',
        finished_at = :finished_at,
        watermark_value = :watermark_value,
        rows_in_stg = :rows_in_stg,
        rows_loaded_to_dwh = :rows_loaded_to_dwh,
        rows_skipped_in_dwh = :rows_skipped_in_dwh,
        error_message = NULL
    WHERE id = :run_id;
    """

    with engine.begin() as conn:
        conn.execute(
            text(update_sql),
            {
                "run_id": run_id,
                "finished_at": datetime.now(),
                "watermark_value": watermark_value,
                "rows_in_stg": rows_in_stg,
                "rows_loaded_to_dwh": rows_loaded_to_dwh,
                "rows_skipped_in_dwh": rows_skipped_in_dwh,
            },
        )

    logger.info(
        f"Pipeline run finished successfully: id={run_id}, status='success'"
    )


def finish_pipeline_run_failed(
    engine: Engine,
    run_id: int,
    error_message: str,
) -> None:
    """
    Updates pipeline run record with failed status
    and error message.
    """
    update_sql = f"""
    UPDATE {settings.pipeline_runs_table}
    SET
        status = 'failed',
        finished_at = :finished_at,
        error_message = :error_message
    WHERE id = :run_id;
    """

    with engine.begin() as conn:
        conn.execute(
            text(update_sql),
            {
                "run_id": run_id,
                "finished_at": datetime.now(),
                "error_message": error_message,
            },
        )

    logger.error(
        f"Pipeline run failed: id={run_id}, status='failed', error='{error_message}'"
    )