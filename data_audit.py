import csv
import json
from collections import defaultdict
from pathlib import Path

# --- CONFIGURATION ---
RAW_DATA_DIR = Path("output/raw")
REPORT_FILE = "dq_audit_report.csv"


def audit_data():
    """
    Scans all JSON files in the raw data directory to identify data quality anomalies.
    Checks strictly for Data Lineage and the 5 core fields (id, name, price, description, images).
    """
    all_json_files = list(RAW_DATA_DIR.rglob("*.json"))

    if not all_json_files:
        print("❌ Không tìm thấy file JSON nào trong cấu trúc thư mục output/raw/!")
        return

    print(f"🔍 Đang phân tích {len(all_json_files)} file JSON...\n")

    seen_ids = set()
    total_records = 0
    duplicates = 0

    anomalies_count = defaultdict(int)
    suspicious_records = []

    for file_path in all_json_files:
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"❌ File JSON bị hỏng (Corrupted): {file_path}")
                continue

            for item in data:
                total_records += 1
                pid = item.get("id")

                # Trích xuất Data Lineage
                api_version = item.get("source_api_version", "UNKNOWN")

                # 1. Kiểm tra trùng lặp ID (Cross-version Duplication)
                if pid in seen_ids:
                    duplicates += 1
                    anomalies_count["duplicate_ids"] += 1
                    continue
                seen_ids.add(pid)

                # 2. Kiểm tra 5 trường dữ liệu cốt lõi
                name = item.get("name", "")
                price = item.get("price")
                desc = item.get("description", "")
                images = item.get("images", [])

                is_suspicious = False
                reasons = []

                # Lỗi Lineage
                if api_version == "UNKNOWN":
                    anomalies_count["missing_lineage"] += 1
                    reasons.append("Missing API Version Tag")
                    is_suspicious = True

                # Lỗi Tên
                if not name or str(name).strip() == "":
                    anomalies_count["missing_name"] += 1
                    reasons.append("Missing Name")
                    is_suspicious = True

                # Lỗi Giá (Null, kiểu chuỗi, hoặc <= 0)
                if price is None or not isinstance(price, (int, float)) or price <= 0:
                    anomalies_count["invalid_price"] += 1
                    reasons.append(f"Invalid Price ({price})")
                    is_suspicious = True

                # Lỗi Mô tả (Trống hoặc quá ngắn < 15 ký tự)
                if not desc or len(str(desc).strip()) < 15:
                    anomalies_count["short_description"] += 1
                    reasons.append("Short/Empty Description")
                    is_suspicious = True

                # Lỗi Hình ảnh (Danh sách rỗng)
                if not images or len(images) == 0:
                    anomalies_count["missing_images"] += 1
                    reasons.append("No Images")
                    is_suspicious = True

                # Ghi nhận record lỗi
                if is_suspicious:
                    suspicious_records.append(
                        {
                            "id": pid,
                            "api_version": api_version,
                            "name": str(name)[:40] + "..." if name else "N/A",
                            "issues": " | ".join(reasons),
                        }
                    )

    # --- IN BÁO CÁO RA TERMINAL ---
    print("=== 📊 DATA QUALITY AUDIT REPORT ===")
    print(f"- Tổng số Record đã quét  : {total_records}")
    print(f"- Số ID Độc bản (Unique)  : {len(seen_ids)}")
    print(f"- Số ID Trùng lặp (Dup)   : {duplicates}")
    print("-" * 40)
    print("🚨 CÁC VẤN ĐỀ PHÁT HIỆN:")
    if not anomalies_count:
        print("  ✅ 100% Dữ liệu sạch, Data Lineage đầy đủ!")
    else:
        for anomaly, count in anomalies_count.items():
            print(f"  - {anomaly}: {count} records")

    # --- XUẤT CSV ---
    if suspicious_records:
        with open(REPORT_FILE, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["id", "api_version", "name", "issues"]
            )
            writer.writeheader()
            writer.writerows(suspicious_records)
        print(
            f"\n📂 Đã lưu {len(suspicious_records)} records nghi ngờ vào {REPORT_FILE} để xử lý."
        )


if __name__ == "__main__":
    audit_data()
