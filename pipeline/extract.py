from pathlib import Path

from pipeline.config import settings
from pipeline.logger_config import get_logger


logger = get_logger("pipeline.extract")


def get_source_file_path() -> Path:
    """
    Returns source file path from config.
    Validates that file exists and is readable.
    """
    source_path = Path(settings.source_file)

    logger.info(f"Source configured: '{source_path}'")

    if not source_path.exists():
        logger.error(f"Source file not found: '{source_path}'")
        raise FileNotFoundError(f"Source file not found: {source_path}")

    if not source_path.is_file():
        logger.error(f"Source path is not a file: '{source_path}'")
        raise FileNotFoundError(f"Source path is not a file: {source_path}")

    try:
        with open(source_path, "rb"):
            pass
    except Exception as e:
        logger.error(f"Source file is not readable: '{source_path}' | error: {e}")
        raise

    logger.info(f"Source file is available for reading: '{source_path}'")

    return source_path