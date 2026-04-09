"""
Data Quality Audit Script.
Task: Cross-reference extracted JSON data against the master ID list.
Output: Comprehensive summary and a list of truly missing IDs for verification.
"""

import csv
import json
import logging
from pathlib import Path
from typing import Set

# ------------------------------------------------------------------------
# DYNAMIC PATH RESOLUTION
# ------------------------------------------------------------------------
# This script is located in 'scripts/', so root is one level up.
ROOT_DIR = Path(__file__).resolve().parent.parent
TARGET_CSV_PATH = ROOT_DIR / "products-0-200000.csv"
JSON_OUTPUT_DIR = ROOT_DIR / "output" / "raw" / "api_v2"
MISSING_IDS_REPORT = ROOT_DIR / "audit_missing_ids.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def get_target_ids() -> Set[str]:
    """Reads the master product list (200,000 IDs)."""
    target_ids = set()
    if not TARGET_CSV_PATH.exists():
        logger.error("Master list not found at: %s", TARGET_CSV_PATH)
        return target_ids

    with open(TARGET_CSV_PATH, "r", encoding="utf-8") as f:
        # Assuming ID is in the first column
        for line in f:
            pid = line.strip().split(",")[0]
            if pid.isdigit():
                target_ids.add(pid)
    return target_ids


def scan_extracted_data() -> tuple[Set[str], int]:
    """Scans all JSON files in the output directory to find successfully scraped IDs."""
    scraped_ids = set()
    duplicate_count = 0

    # Recursive scan to cover all year/month/day subfolders
    json_files = list(JSON_OUTPUT_DIR.rglob("*.json"))
    logger.info("Starting audit of %d JSON files...", len(json_files))

    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    pid = str(item.get("id"))
                    if pid in scraped_ids:
                        duplicate_count += 1
                    else:
                        scraped_ids.add(pid)
        except (json.JSONDecodeError, IOError):
            logger.warning("Skipping corrupted file: %s", file_path.name)

    return scraped_ids, duplicate_count


def main():
    """Main Audit Workflow."""
    print("\n--- TIKI DATA EXTRACTION AUDIT ---")

    # 1. Load Master List
    target_ids = get_target_ids()
    if not target_ids:
        return

    # 2. Scan Output Folder
    scraped_ids, duplicates = scan_extracted_data()

    # 3. Calculate Results
    total_target = len(target_ids)
    total_found = len(scraped_ids)
    missing_ids = target_ids - scraped_ids
    yield_rate = (total_found / total_target) * 100

    # 4. Save Missing IDs for Phase 5 (Verification)
    if missing_ids:
        with open(MISSING_IDS_REPORT, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id"])
            for pid in sorted(list(missing_ids)):
                writer.writerow([pid])

    # 5. Final Report
    print("==================================================")
    print(f"Target Population:      {total_target:,}")
    print(f"Unique Extracted:       {total_found:,}")
    print(f"Duplicates Removed:     {duplicates:,}")
    print(f"Missing/Dead IDs:       {len(missing_ids):,}")
    print(f"Success Rate (Yield):   {yield_rate:.2f}%")
    print("==================================================")

    if missing_ids:
        print(f"Check '{MISSING_IDS_REPORT.name}' for IDs to verify on web.\n")


if __name__ == "__main__":
    main()
