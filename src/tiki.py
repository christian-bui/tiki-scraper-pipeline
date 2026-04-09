"""
Core Engine: Tiki Pipeline.
Provides the class and methods to scrape, validate, and store Tiki products.
"""

import asyncio
import csv
import json
import logging
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set

import aiohttp

# Import natively without sys.path hacks because main.py runs from the root
from config import (
    API_BASE_URL,
    API_VERSION,
    BATCH_DELAY,
    CHUNK_SIZE,
    OUTPUT_ROOT,
    SEMAPHORE_LIMIT,
    TIMEOUT_SECONDS,
)
from src.utils import clean_html

logger = logging.getLogger(__name__)

# Determine ROOT_DIR cleanly based on this file's absolute position
ROOT_DIR = Path(__file__).resolve().parent.parent


class TikiPipeline:
    """Class to manage the web scraping process."""

    def __init__(self):
        self.semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

        self.daily_path = self._initialize_partition()
        self.daily_path.mkdir(parents=True, exist_ok=True)

        self.global_api_path = ROOT_DIR / OUTPUT_ROOT / f"api_{API_VERSION}"
        self.global_api_path.mkdir(parents=True, exist_ok=True)

        self.success_log = self.global_api_path / "success_ids.txt"
        self.fail_log = self.daily_path / "all_failed_ids.csv"

    def _initialize_partition(self) -> Path:
        now = datetime.now()
        return (
            ROOT_DIR
            / OUTPUT_ROOT
            / f"api_{API_VERSION}"
            / f"year={now.year}"
            / f"month={now.strftime('%m')}"
            / f"day={now.strftime('%d')}"
        )

    def _get_processed_ids(self) -> Set[str]:
        processed = set()
        if self.success_log.exists():
            with open(self.success_log, "r", encoding="utf-8") as f:
                processed.update(line.strip() for line in f if line.strip())

        if self.fail_log.exists():
            try:
                with open(self.fail_log, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        processed.add(str(row["id"]))
            except (csv.Error, IOError):
                pass
        return processed

    async def fetch_product(
        self, session: aiohttp.ClientSession, pid: str, retries: int = 3
    ) -> Dict[str, Any]:
        url = f"{API_BASE_URL}/{pid}"
        headers = {
            "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(115, 123)}.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://tiki.vn/",
            "Connection": "keep-alive",
        }
        last_error_msg = "UNKNOWN_ERROR"

        async with self.semaphore:
            for attempt in range(retries):
                try:
                    async with session.get(
                        url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()

                            p_id = data.get("id")
                            raw_name = data.get("name")
                            name = (
                                raw_name.strip()
                                if isinstance(raw_name, str)
                                else raw_name
                            )
                            url_key = data.get("url_key")
                            description = clean_html(data.get("description"))

                            raw_price = str(data.get("price", "0"))
                            clean_price_str = re.sub(r"\D", "", raw_price)
                            final_price = int(clean_price_str) if clean_price_str else 0

                            raw_images = data.get("images") or []
                            images_url = [
                                img.get("base_url")
                                for img in raw_images
                                if isinstance(img, dict) and img.get("base_url")
                            ]

                            check_list = [
                                p_id,
                                name,
                                url_key,
                                final_price,
                                description,
                                images_url,
                            ]
                            if sum(1 for item in check_list if item) >= 2:
                                return {
                                    "id": pid,
                                    "status": "SUCCESS",
                                    "data": {
                                        "id": p_id,
                                        "name": name,
                                        "url_key": url_key,
                                        "price": final_price,
                                        "description": description,
                                        "images_url": images_url,
                                        "scraped_at": datetime.now().isoformat(),
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
                        elif resp.status == 429:
                            last_error_msg = "429_RATE_LIMIT"
                            await asyncio.sleep(15)
                        else:
                            last_error_msg = f"HTTP_{resp.status}"
                            resp.raise_for_status()

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_error_msg = type(e).__name__
                    await asyncio.sleep(random.uniform(1, 3))

        return {
            "id": pid,
            "status": "FAILED",
            "error": f"RETRY_EXHAUSTED_{last_error_msg}",
        }

    def _get_next_batch_index(self) -> int:
        files = list(self.daily_path.glob("batch_*.json"))
        indices = [int(f.stem.split("_")[1]) for f in files if "_" in f.stem]
        return max(indices) + 1 if indices else 1

    def _persist_batch_sync(self, batch_idx: int, success: list, failed: list):
        if success:
            output_file = self.daily_path / f"batch_{batch_idx}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(success, f, ensure_ascii=False, indent=4)
            with open(self.success_log, "a", encoding="utf-8") as f:
                f.write("\n".join(str(item["id"]) for item in success) + "\n")

        if failed:
            exists = self.fail_log.exists() and self.fail_log.stat().st_size > 0
            with open(self.fail_log, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "error"])
                if not exists:
                    writer.writeheader()
                writer.writerows(failed)

    async def run(self, ids_to_run: List[str], is_retry_phase: bool = False):
        total = len(ids_to_run)
        if total == 0:
            return

        phase = "RETRY_PHASE" if is_retry_phase else "MAIN_PHASE"
        logger.info("EXECUTION_START | PHASE: %s | TOTAL_IDS: %d", phase, total)

        cumulative_success = 0
        cumulative_failed = 0
        start_idx = self._get_next_batch_index()

        async with aiohttp.ClientSession() as session:
            for i in range(0, total, CHUNK_SIZE):
                batch_ids = ids_to_run[i : i + CHUNK_SIZE]
                tasks = [self.fetch_product(session, pid) for pid in batch_ids]
                results = await asyncio.gather(*tasks)

                success = [r["data"] for r in results if r["status"] == "SUCCESS"]
                failed = [
                    {"id": r["id"], "error": r["error"]}
                    for r in results
                    if r["status"] == "FAILED"
                ]

                cumulative_success += len(success)
                cumulative_failed += len(failed)

                await asyncio.to_thread(
                    self._persist_batch_sync,
                    start_idx + (i // CHUNK_SIZE),
                    success,
                    failed,
                )

                logger.info(
                    "PROGRESS | %s | %d/%d | TOTAL_SUCCESS: %d | TOTAL_FAILED: %d",
                    phase,
                    min(i + CHUNK_SIZE, total),
                    total,
                    cumulative_success,
                    cumulative_failed,
                )
                await asyncio.sleep(BATCH_DELAY)

        logger.info(
            "PHASE_SUMMARY | %s IS COMPLETED | FINAL_SUCCESS: %d | FINAL_FAILED: %d",
            phase,
            cumulative_success,
            cumulative_failed,
        )
