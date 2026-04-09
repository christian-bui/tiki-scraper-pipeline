"""
Frontend Web Audit Script.

Validates the existence of product IDs by hitting the Tiki frontend web URLs.
Targets IDs that failed in the API pipeline and are NOT in success_ids.txt.
"""

import asyncio
import csv
import logging
from pathlib import Path
from typing import List, Set

import aiohttp

# ------------------------------------------------------------------------
# CONFIGURATIONS
# ------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent.parent
API_DATA_DIR = (
    ROOT_DIR / "output" / "raw" / "api_v2" / "year=2026" / "month=04" / "day=06"
)
SUCCESS_LOG_PATH = API_DATA_DIR / "success_ids.txt"
FAILED_LOG_PATH = API_DATA_DIR / "all_failed_ids_old.csv"
AUDIT_RESULT_PATH = ROOT_DIR / "frontend_audit_results.csv"

CONCURRENCY_LIMIT = 2
DELAY_BETWEEN_REQUESTS = 1.0
AUDIT_LIMIT = None  # Set to an integer to test a subset, or None for all.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------
# CORE LOGIC
# ------------------------------------------------------------------------


async def verify_url(session: aiohttp.ClientSession, pid: str) -> str:
    """
    Hits the product's web URL to verify its active status.
    Uses ClientTimeout object to comply with aiohttp requirements.
    """
    url = f"https://tiki.vn/product-p{pid}.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    # Requirement: aiohttp requires a ClientTimeout object
    timeout_config = aiohttp.ClientTimeout(total=10)

    try:
        async with session.get(
            url, headers=headers, allow_redirects=True, timeout=timeout_config
        ) as resp:
            final_url = str(resp.url)
            status = resp.status

            if status == 404:
                return "DEAD_404_NOT_FOUND"

            if final_url == "https://tiki.vn/" or "khong-tim-thay" in final_url:
                return "DEAD_REDIRECTED_TO_HOME"

            if status == 200:
                return "ALIVE_PAGE_EXISTS"

            return f"UNKNOWN_HTTP_{status}"

    except Exception as e:
        return f"ERROR_{type(e).__name__}"


async def main():
    """Main execution workflow for the frontend audit."""
    logger.info("Initializing Frontend Audit Pipeline...")

    # 1. Load successful IDs to skip them
    success_set: Set[str] = set()
    if SUCCESS_LOG_PATH.exists():
        with open(SUCCESS_LOG_PATH, "r", encoding="utf-8") as f:
            success_set.update(line.strip() for line in f if line.strip())

    logger.info("Loaded %d IDs from the global success cache.", len(success_set))

    # 2. Extract only IDs that are still missing
    pending_ids: List[str] = []
    if FAILED_LOG_PATH.exists():
        with open(FAILED_LOG_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = str(row.get("id", "")).strip()
                if pid and pid not in success_set:
                    pending_ids.append(pid)

    total_pending = len(pending_ids)
    if total_pending == 0:
        logger.info("Audit complete. No pending failed IDs found.")
        return

    logger.info("Found %d unresolved failed IDs for audit.", total_pending)

    if AUDIT_LIMIT:
        pending_ids = pending_ids[:AUDIT_LIMIT]
        logger.info("Audit limit applied: Processing %d IDs.", AUDIT_LIMIT)

    file_exists = AUDIT_RESULT_PATH.exists()
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        # Open file in append mode to ensure data safety
        with open(AUDIT_RESULT_PATH, "a", encoding="utf-8", newline="") as csvfile:
            fieldnames = ["id", "web_status"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()

            for idx, pid in enumerate(pending_ids, 1):
                async with semaphore:
                    status = await verify_url(session, pid)

                    writer.writerow({"id": pid, "web_status": status})
                    csvfile.flush()  # Immediate disk write

                    logger.info(
                        "[%d/%d] ID: %s | Status: %s", idx, total_pending, pid, status
                    )

                    # Throttle to protect IP reputation
                    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    logger.info("Audit finished. Results saved to: %s", AUDIT_RESULT_PATH.name)


if __name__ == "__main__":
    asyncio.run(main())
