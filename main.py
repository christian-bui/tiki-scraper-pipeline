import asyncio
import csv
import json
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
from bs4 import BeautifulSoup

# --- PIPELINE CONFIGURATION ---
CHUNK_SIZE = 1000
SEMAPHORE_LIMIT = 50
TIMEOUT_SECONDS = 15
INPUT_FILE = "products-0-200000.csv"
OUTPUT_ROOT = Path("output/raw")

# Quyết định version tại một nơi duy nhất
API_VERSION = "v2"
API_BASE_URL = f"https://api.tiki.vn/product-detail/api/{API_VERSION}/products"

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class TikiPipeline:
    """
    Asynchronous ETL pipeline for large-scale Tiki product extraction.
    Implements: Data Lineage (API versioning), Concurrency, and Partitioning.
    """

    def __init__(self):
        self.semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
        self.daily_path = self._initialize_partition()
        self.daily_path.mkdir(parents=True, exist_ok=True)

    def _initialize_partition(self) -> Path:
        """Generates partitioned structure including API version: output/raw/api_v2/year=..."""
        now = datetime.now()
        # Thêm API_VERSION vào cấu trúc thư mục Data Lake
        return (
            OUTPUT_ROOT
            / f"api_{API_VERSION}"
            / f"year={now.year}"
            / f"month={now.strftime('%m')}"
            / f"day={now.strftime('%d')}"
        )

    def clean_html(self, html: str) -> str:
        """Extracts plain text from raw HTML content using BeautifulSoup."""
        if not html:
            return ""
        return BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)

    async def fetch_product(
        self, session: aiohttp.ClientSession, pid: str, retries: int = 3
    ) -> Dict[str, Any]:
        """Fetches and normalizes product detail from Tiki API."""
        url = f"{API_BASE_URL}/{pid}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        last_error = "Unknown Error"

        async with self.semaphore:
            for attempt in range(retries):
                try:
                    async with session.get(
                        url, headers=headers, timeout=TIMEOUT_SECONDS
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()

                            # Validation: Ensure critical data exists
                            if not data.get("name") or data.get("price") is None:
                                return {
                                    "id": pid,
                                    "status": "FAILED",
                                    "error": "200 OK but Missing Name/Price",
                                    "version": API_VERSION,
                                }

                            return {
                                "id": pid,
                                "status": "SUCCESS",
                                "data": {
                                    # Data Lineage: Gắn mã nguồn vào từng record
                                    "source_api_version": API_VERSION,
                                    "scraped_at": datetime.now().isoformat(),
                                    # Extracted Fields
                                    "id": data.get("id"),
                                    "sku": data.get("sku"),
                                    "name": data.get("name"),
                                    "brand_name": data.get("brand", {}).get("name"),
                                    "price": data.get("price"),
                                    "list_price": data.get("list_price"),
                                    "discount_rate": data.get("discount_rate"),
                                    "rating": data.get("rating_average"),
                                    "reviews": data.get("review_count"),
                                    "inventory_status": data.get("inventory_status"),
                                    "categories": [
                                        b.get("name")
                                        for b in data.get("breadcrumbs", [])
                                    ],
                                    "description": self.clean_html(
                                        data.get("description")
                                    ),
                                    "images": [
                                        img.get("base_url")
                                        for img in data.get("images", [])
                                    ],
                                },
                            }

                        elif resp.status == 404:
                            logger.warning(
                                f"Product {pid} not found (404) on {API_VERSION}"
                            )
                            return {
                                "id": pid,
                                "status": "FAILED",
                                "error": "404 Not Found",
                                "version": API_VERSION,
                            }

                        else:
                            last_error = f"HTTP {resp.status}"
                            resp.raise_for_status()

                except asyncio.TimeoutError:
                    last_error = "Timeout Error"
                except aiohttp.ClientResponseError as e:
                    last_error = f"HTTP Error {e.status}"
                except Exception as e:
                    last_error = f"Network/System Exception: {str(e)}"

                await asyncio.sleep(random.uniform(0.5, 1.5))

        return {
            "id": pid,
            "status": "FAILED",
            "error": f"Max retries reached. Last issue: {last_error}",
            "version": API_VERSION,
        }

    async def run(self, limit: Optional[int] = None):
        """Orchestrates the scraping process in batches."""
        try:
            with open(INPUT_FILE, mode="r", encoding="utf-8") as f:
                all_ids = [row["id"] for row in csv.DictReader(f)]
        except FileNotFoundError:
            logger.error(f"Source file {INPUT_FILE} missing.")
            return

        if limit:
            all_ids = all_ids[:limit]
            logger.info(f"TEST MODE: {limit} IDs on {API_VERSION}")

        total = len(all_ids)
        logger.info(f"Starting pipeline. Total: {total} | API: {API_VERSION}")

        async with aiohttp.ClientSession() as session:
            for i in range(0, total, CHUNK_SIZE):
                batch_ids = all_ids[i : i + CHUNK_SIZE]
                tasks = [self.fetch_product(session, pid) for pid in batch_ids]
                results = await asyncio.gather(*tasks)

                success_batch = [r["data"] for r in results if r["status"] == "SUCCESS"]
                failed_batch = [
                    {
                        "id": r["id"],
                        "api_version": r.get("version"),
                        "error": r["error"],
                    }
                    for r in results
                    if r["status"] == "FAILED"
                ]

                self._persist_batch(i // CHUNK_SIZE + 1, success_batch, failed_batch)
                logger.info(
                    f"Progress: {min(i + CHUNK_SIZE, total)}/{total} processed."
                )

    def _persist_batch(self, batch_idx: int, success: list, failed: list):
        """Saves batch results to disk."""
        if success:
            output_file = self.daily_path / f"batch_{batch_idx}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(success, f, ensure_ascii=False, indent=4)

        if failed:
            fail_log = self.daily_path / "all_failed_ids.csv"
            exists = fail_log.exists()
            with open(fail_log, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "api_version", "error"])
                if not exists:
                    writer.writeheader()
                writer.writerows(failed)


if __name__ == "__main__":
    pipeline = TikiPipeline()
    # Chạy thật: Sửa thành limit=None
    asyncio.run(pipeline.run(limit=None))
