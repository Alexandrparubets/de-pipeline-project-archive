from pipeline.logger_config import get_logger, set_run_id


logger = get_logger("pipeline.run")


def run_pipeline() -> None:
   
    logger.info("🚀 Pipeline process started")

    # дальше будет логика пайплайна

    logger.info("✅ Pipeline finished")


if __name__ == "__main__":
    run_pipeline()