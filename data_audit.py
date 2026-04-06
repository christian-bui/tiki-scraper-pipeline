"""
Utility script to audit scraped Tiki product data for quality, duplicates,
and overall pipeline reconciliation.
Automatically finds the latest available data partition.
"""

import csv
import glob
import json
from pathlib import Path

import pandas as pd


def get_latest_data_partition(root_path: str) -> str | None:
    """
    Search the directory tree for the most recent folder containing batch_*.json files.
    This eliminates the need for hardcoded dates in the script.
    """
    base_path = Path(root_path)
    if not base_path.exists():
        return None

    # Find all directories starting with 'day=' and sort in descending order (latest first)
    all_day_dirs = sorted(base_path.rglob("day=*"), reverse=True)

    for directory in all_day_dirs:
        # Return the directory only if it contains actual JSON batch files
        if list(directory.glob("batch_*.json")):
            return str(directory)

    return None


def audit_data(data_path: str) -> None:
    """
    Perform comprehensive data quality checks including:
    - Duplicate detection
    - Reconciliation between successful and failed records
    - Pipeline success rate calculation
    """
    print(f"AUDIT_START: Scanning data directory -> {data_path}")

    json_files = glob.glob(f"{data_path}/batch_*.json")
    if not json_files:
        print("AUDIT_FAILED: No JSON batch files found in this directory.")
        return

    all_items = []
    anomalies = []
    seen_ids = set()
    duplicate_count = 0

    # 1. SCAN SUCCESSFUL DATA (JSON)
    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                batch = json.load(f)
                for item in batch:
                    pid = str(item.get("id"))

                    # Check for duplicate IDs
                    if pid in seen_ids:
                        duplicate_count += 1
                        anomalies.append(
                            {
                                "product_id": pid,
                                "issue": "DUPLICATE_ID",
                                "file": Path(file_path).name,
                            }
                        )
                    else:
                        seen_ids.add(pid)

                    # Check for suspicious values (e.g., price > 200 Million VND)
                    price = item.get("price") or 0
                    if price > 200000000:
                        anomalies.append(
                            {
                                "product_id": pid,
                                "issue": f"SUSPICIOUS_HIGH_PRICE: {price}",
                                "file": Path(file_path).name,
                            }
                        )

                    all_items.append(item)
        except (json.JSONDecodeError, IOError) as e:
            print(f"ERROR: Failed to read file {file_path}: {e}")

    total_success = len(all_items)
    unique_success = total_success - duplicate_count

    # 2. SCAN FAILED DATA (CSV)
    fail_log = Path(data_path) / "all_failed_ids.csv"
    total_failed = 0
    if fail_log.exists():
        try:
            with open(fail_log, "r", encoding="utf-8") as f:
                # Count rows excluding the header
                total_failed = sum(1 for _ in csv.DictReader(f))
        except (IOError, csv.Error):
            pass

    # 3. CALCULATE RECONCILIATION METRICS
    total_processed = unique_success + total_failed
    success_rate = (
        (unique_success / total_processed * 100) if total_processed > 0 else 0
    )

    print("\n" + "=" * 60)
    print("DATA RECONCILIATION REPORT")
    print("=" * 60)
    print(f"Total Records Scanned (JSON):    {total_success}")
    print(f"Duplicate Records Found:         {duplicate_count}")
    print(f"Total UNIQUE Success IDs:        {unique_success}")
    print(f"Total Failed IDs (CSV Log):      {total_failed}")
    print("-" * 60)
    print(f"Total Pipeline Throughput:       {total_processed} IDs")
    print(f"Pipeline Success Rate:           {success_rate:.2f}%")
    print("=" * 60)

    # 4. EXPORT ANOMALY REPORT
    if anomalies:
        print(f"\nALERT: Detected {len(anomalies)} anomalies (Duplicates/Outliers).")
        df_err = pd.DataFrame(anomalies)

        report_file = Path(data_path) / "audit_anomalies_report.csv"
        df_err.to_csv(report_file, index=False)
        print(f"DETAILED REPORT SAVED AT: {report_file}")
    else:
        print("\nAUDIT PASSED: Data is clean, no duplicates, no outliers.")


if __name__ == "__main__":
    # Root path containing raw output data
    ROOT_DATA = "output/raw/api_v2"

    # Dynamically locate the most recent partition
    latest_dir = get_latest_data_partition(ROOT_DATA)

    if latest_dir:
        audit_data(latest_dir)
    else:
        print(f"AUDIT_FAILED: No data directories found under {ROOT_DATA}")
