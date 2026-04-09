"""
Advanced manual retry script featuring:
1. Auto-Resume: Automatically detects the latest retry_failed_N.csv to resume from.
2. Loosely Coupled Data: Accepts item if it has ID + at least 1 out of 5 core fields.
3. In-Memory Buffer: Accumulates exactly 1000 successful items before I/O flush.
4. Anti-Ban System: Circuit Breaker, Exponential Backoff Jitter, and UA Rotation.
"""

import sys
from pathlib import Path

# Resolve root directory (scripts/retries/ -> scripts/ -> root)
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import asyncio
import csv
import json
import logging
import random
from typing import Any, Dict, List, Optional

import aiohttp

# Import from the root config and the core engine
from config import API_BASE_URL, TIMEOUT_SECONDS
from src.tiki import TikiPipeline

# Real User-Agents from 2026 to bypass basic WAF fingerprinting
REAL_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


class CustomRetryPipeline(TikiPipeline):
    """Extended pipeline for handling memory buffering and Anti-Ban systems."""

    def __init__(self):
        super().__init__()
        # self.success_log is automatically inherited from TikiPipeline as the Global Path
        self.success_buffer: List[Dict[str, Any]] = []
        self.current_round_successes = 0
        self.retry_fail_log: Optional[Path] = None

        # Anti-Ban State
        self.consecutive_blocks = 0
        self.circuit_breaker_tripped = False

    def set_round_config(self, round_num: int):
        """Initialize the output path for the current retry round."""
        self.retry_fail_log = self.daily_path / f"retry_failed_{round_num}.csv"
        if self.retry_fail_log.exists():
            self.retry_fail_log.unlink()
        self.current_round_successes = 0
        logger.info("ROUND_CONFIG | Failure log set to: %s", self.retry_fail_log.name)

    def _get_next_batch_index(self) -> int:
        """Scan directory for 'batch_retry_m.json' and return the next index."""
        files = list(self.daily_path.glob("batch_retry_*.json"))
        indices = []
        for f in files:
            try:
                m_part = f.stem.split("_")[-1]
                indices.append(int(m_part))
            except (ValueError, IndexError):
                continue
        return max(indices) + 1 if indices else 1

    def _save_buffer_chunk(self, chunk: list):
        """Flush a RAM chunk to a JSON file and append IDs to the global master list."""
        m = self._get_next_batch_index()
        output_file = self.daily_path / f"batch_retry_{m}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=4)

        # Append successful IDs to the global cache (api_v2/success_ids.txt)
        with open(self.success_log, "a", encoding="utf-8") as f:
            f.write("\n".join(str(item["id"]) for item in chunk) + "\n")

        logger.info(
            "BUFFER_FLUSH | Created %s with %d items.", output_file.name, len(chunk)
        )

    def _persist_batch_sync(self, batch_idx: int, success: list, failed: list):
        """Handle the 1000-item buffer logic and dynamic failure routing."""
        self.current_round_successes += len(success)

        # Buffer Logic: Save only when reaching exactly 1000 items
        self.success_buffer.extend(success)
        while len(self.success_buffer) >= 1000:
            chunk_to_save = self.success_buffer[:1000]
            self.success_buffer = self.success_buffer[1000:]
            self._save_buffer_chunk(chunk_to_save)

        # Failure Logic: Route to the specific round CSV
        if failed and self.retry_fail_log is not None:
            exists = (
                self.retry_fail_log.exists() and self.retry_fail_log.stat().st_size > 0
            )
            with open(self.retry_fail_log, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "error"])
                if not exists:
                    writer.writeheader()
                writer.writerows(failed)

    def flush_remaining_buffer(self):
        """Save any leftover items (< 1000) at the end of execution."""
        if self.success_buffer:
            logger.info(
                "FINAL_FLUSH | Saving remaining %d items.", len(self.success_buffer)
            )
            self._save_buffer_chunk(self.success_buffer)
            self.success_buffer.clear()

    async def fetch_product(
        self, session: aiohttp.ClientSession, pid: str, retries: int = 5
    ) -> dict:
        """Custom fetch with Circuit Breaker, Exponential Backoff, and UA Rotation."""
        url = f"{API_BASE_URL}/{pid}"

        headers = {
            "User-Agent": random.choice(REAL_USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://tiki.vn/",
        }
        last_error_msg = "UNKNOWN_ERROR"

        async with self.semaphore:
            for attempt in range(retries):
                if self.circuit_breaker_tripped:
                    await asyncio.sleep(60)
                    self.circuit_breaker_tripped = False

                try:
                    async with session.get(
                        url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                    ) as resp:
                        if resp.status == 200:
                            self.consecutive_blocks = 0
                            data = await resp.json()

                            p_id = data.get("id")
                            name = (
                                data.get("name", "").strip() if data.get("name") else ""
                            )
                            url_key = data.get("url_key")
                            price = data.get("price")
                            description = data.get("description")
                            images = data.get("images")

                            # Loosely Coupled Validation
                            has_valid_extra_field = any(
                                [
                                    bool(name),
                                    url_key is not None,
                                    price is not None,
                                    description is not None,
                                    images is not None,
                                ]
                            )

                            if p_id and has_valid_extra_field:
                                return {
                                    "id": pid,
                                    "status": "SUCCESS",
                                    "data": {
                                        "id": p_id,
                                        "name": name,
                                        "url_key": url_key,
                                        "price": price,
                                        "description": description,
                                        "images": images,
                                        "scraped_at": data.get("scraped_at"),
                                    },
                                }
                            return {
                                "id": pid,
                                "status": "FAILED",
                                "error": "DATA_TOO_SPARSE",
                            }

                        elif resp.status == 404:
                            return {
                                "id": pid,
                                "status": "FAILED",
                                "error": "404_NOT_FOUND",
                            }

                        elif resp.status in (429, 403):
                            self.consecutive_blocks += 1
                            if self.consecutive_blocks > 20:
                                self.circuit_breaker_tripped = True

                            delay = (2**attempt) + random.uniform(1.0, 3.0)
                            await asyncio.sleep(delay)
                        else:
                            resp.raise_for_status()

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_error_msg = type(e).__name__
                    await asyncio.sleep(random.uniform(1, 3))

        return {
            "id": pid,
            "status": "FAILED",
            "error": f"RETRY_EXHAUSTED_{last_error_msg}",
        }


def get_startup_config(base_dir: Path) -> tuple[Optional[Path], int]:
    """
    Scan the directory to find the starting point and round number.
    Priority 1: retry_failed_N.csv (Highest N) -> Start round N + 1
    Priority 2: all_failed_ids_old.csv -> Start round 1
    Priority 3: all_failed_ids.csv -> Start round 1
    """
    if not base_dir.exists():
        return None, 1

    retry_files = list(base_dir.glob("retry_failed_*.csv"))
    if retry_files:
        indices = []
        for f in retry_files:
            try:
                num = int(f.stem.replace("retry_failed_", ""))
                indices.append(num)
            except ValueError:
                continue

        if indices:
            max_n = max(indices)
            return base_dir / f"retry_failed_{max_n}.csv", max_n + 1

    old_fail_file = base_dir / "all_failed_ids_old.csv"
    if old_fail_file.exists():
        return old_fail_file, 1

    normal_fail_file = base_dir / "all_failed_ids.csv"
    if normal_fail_file.exists():
        return normal_fail_file, 1

    return None, 1


async def main():
    """Manages the infinite retry loop and convergence conditions."""

    # Target directory to run retries against (Update this as needed)
    base_target_dir = (
        ROOT_DIR / "output" / "raw" / "api_v2" / "year=2026" / "month=04" / "day=06"
    )

    current_input_file, round_num = get_startup_config(base_target_dir)

    if current_input_file is None or not current_input_file.exists():
        logger.error("SYSTEM_HALTED | No valid failed log found in %s", base_target_dir)
        return

    logger.info(
        "SYSTEM_STARTUP | Resuming from: %s (Round %d)",
        current_input_file.name,
        round_num,
    )

    pipeline = CustomRetryPipeline()

    while True:
        if current_input_file is None:
            logger.info("CHECKPOINT | Input file became None. Breaking loop.")
            break

        retry_ids = []
        try:
            with open(current_input_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    retry_ids.append(row["id"])
        except (csv.Error, IOError):
            break

        if not retry_ids:
            logger.info("CONVERGENCE_REACHED | No more IDs to process.")
            break

        logger.info("==================================================")
        logger.info(
            "ITERATION_START | Round: %d | Input: %s | Count: %d",
            round_num,
            current_input_file.name,
            len(retry_ids),
        )

        pipeline.set_round_config(round_num)
        await pipeline.run(retry_ids, is_retry_phase=True)

        new_successes = pipeline.current_round_successes

        # STOP CONDITION: > 10 rounds AND 0 new successes
        if round_num > 10 and new_successes == 0:
            logger.info(
                "TERMINATION_CRITERIA_MET | Reached round %d with 0 success. Exiting loop.",
                round_num,
            )
            break

        current_input_file = pipeline.retry_fail_log
        round_num += 1

    pipeline.flush_remaining_buffer()
    logger.info("PIPELINE_FINALIZED | Manual retry loop finished.")


if __name__ == "__main__":
    asyncio.run(main())
