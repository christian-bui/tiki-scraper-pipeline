"""
Main Entry Point.
Orchestrates the Tiki scraping pipeline.
"""

import asyncio
import csv
import logging
from pathlib import Path

# Clean imports straight from the root
from config import INPUT_FILE, LIMIT
from src.tiki import TikiPipeline

# Setup global logging at the root directory
ROOT_DIR = Path(__file__).resolve().parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(ROOT_DIR / "pipeline.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


async def main():
    """Main execution workflow."""
    pipeline = TikiPipeline()
    input_path = ROOT_DIR / INPUT_FILE

    try:
        with open(input_path, mode="r", encoding="utf-8-sig") as f:
            all_ids = list(dict.fromkeys([row["id"] for row in csv.DictReader(f)]))
    except FileNotFoundError:
        logger.error("IO_ERROR: Input file %s not found.", input_path)
        return

    processed = pipeline._get_processed_ids()
    remaining = [pid for pid in all_ids if str(pid) not in processed]

    if LIMIT is not None:
        remaining = remaining[:LIMIT]
        logger.info("MODE_TEST: Applied limit of %d IDs", LIMIT)

    # 1. Main Phase
    if remaining:
        await pipeline.run(remaining)
    else:
        logger.info("CHECKPOINT: All IDs processed in Main Phase.")

    # 2. Local Retry Phase
    logger.info("INITIATING_RETRY_SCAN")
    if pipeline.fail_log.exists():
        backup_log = pipeline.daily_path / "all_failed_ids_old.csv"
        pipeline.fail_log.rename(backup_log)

        with open(backup_log, "r", encoding="utf-8") as f:
            failed_list = [
                row["id"] for row in csv.DictReader(f) if "404" not in row["error"]
            ]

        if failed_list:
            await pipeline.run(failed_list, is_retry_phase=True)

    logger.info("PIPELINE_COMPLETELY_FINISHED")


if __name__ == "__main__":
    asyncio.run(main())
