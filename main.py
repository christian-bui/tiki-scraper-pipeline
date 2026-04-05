import asyncio
import csv
import glob
import json
import os
import random
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup

# --- CẤU HÌNH ---
CHUNK_SIZE = 1000
SEMAPHORE_LIMIT = 50
TIMEOUT_SECONDS = 15
ORIGINAL_CSV = "products-0-200000.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]


def clean_description(html_content):
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text(separator=" ", strip=True)


# BÁO CÁO TIẾN ĐỘ TỪNG LÔ
def log_chunk_completion(chunk_index, success_count, log_file="scraping_report.txt"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(
            f"[{now}] Lô {chunk_index}: Tải xong {success_count} sản phẩm thành công.\n"
        )


async def fetch_product(session, product_id, semaphore):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    async with semaphore:
        try:
            async with session.get(
                url, headers=headers, timeout=TIMEOUT_SECONDS
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    name, price = data.get("name"), data.get("price")
                    if not name or price is None:
                        return {
                            "id": product_id,
                            "error": "Thiếu Tên/Giá",
                            "type": "DATA_ISSUE",
                        }
                    return {
                        "status": "SUCCESS",
                        "data": {
                            "id": data.get("id"),
                            "name": name,
                            "price": price,
                            "description": clean_description(data.get("description")),
                            "images_url": [
                                img.get("base_url") for img in data.get("images", [])
                            ],
                        },
                    }
                return {
                    "id": product_id,
                    "error": f"HTTP {response.status}",
                    "type": "SERVER_ERROR",
                }
        except Exception as e:
            return {"id": product_id, "error": str(e), "type": "NETWORK_ISSUE"}


async def process_and_save_chunk(session, chunk_ids, chunk_index, semaphore):
    tasks = [fetch_product(session, pid, semaphore) for pid in chunk_ids]
    results = await asyncio.gather(*tasks)
    success_data = [
        res["data"] for res in results if res and res.get("status") == "SUCCESS"
    ]
    failed_ids = [
        res["id"] for res in results if not res or res.get("status") != "SUCCESS"
    ]

    if success_data:
        with open(f"tiki_products_part_{chunk_index}.json", "w", encoding="utf-8") as f:
            json.dump(success_data, f, ensure_ascii=False, indent=4)
        log_chunk_completion(
            chunk_index, len(success_data)
        )  # Ghi file txt báo cáo tiến độ

    print(f"-> Part {chunk_index}: Xong {len(success_data)} SP | Lỗi {len(failed_ids)}")
    return failed_ids


# BÁO CÁO TỔNG KẾT CUỐI CÙNG (DEDUPLICATE + FAILED CSV)
def final_consolidation(original_ids):
    print("\n[BÁO CÁO TỔNG KẾT] Đang xử lý dữ liệu cuối cùng...")
    file_list = glob.glob("tiki_products_part_*.json")
    if os.path.exists("tiki_products_retry_success.json"):
        file_list.append("tiki_products_retry_success.json")

    unique_products = {}

    def get_score(p):
        return sum(1 for v in p.values() if v)

    for filename in file_list:
        with open(filename, "r", encoding="utf-8") as f:
            for item in json.load(f):
                pid = str(item["id"])
                if pid not in unique_products or get_score(item) >= get_score(
                    unique_products[pid]
                ):
                    unique_products[pid] = item

    # 1. Lưu file JSON sạch
    final_data = list(unique_products.values())
    with open("tiki_products_FINAL.json", "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)

    # 2. Tìm và xuất file CSV các ID thất bại
    success_ids = set(unique_products.keys())
    failed_to_download = [pid for pid in original_ids if pid not in success_ids]

    with open("failed_products.csv", "w", encoding="utf-8") as f:
        f.write("id\n")
        for pid in failed_to_download:
            f.write(f"{pid}\n")

    print("-" * 30)
    print("TỔNG KẾT:")
    print(f"- Tổng ID gốc: {len(original_ids)}")
    print(f"- Tổng ID tải thành công: {len(final_data)}")
    print(f"- Tổng ID thất bại: {len(failed_to_download)}")
    print("-> File sạch: tiki_products_FINAL.json")
    print("-> File lỗi: failed_products.csv")
    print("-" * 30)


async def main():
    all_ids = []
    with open(ORIGINAL_CSV, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_ids = [row["id"] for row in reader]

    total = len(all_ids)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    all_failed_ids = []

    async with aiohttp.ClientSession() as session:
        for i in range(0, total, CHUNK_SIZE):
            chunk, idx = all_ids[i : i + CHUNK_SIZE], (i // CHUNK_SIZE) + 1
            if os.path.exists(f"tiki_products_part_{idx}.json"):
                continue

            f_ids = await process_and_save_chunk(session, chunk, idx, semaphore)
            all_failed_ids.extend(f_ids)
            await asyncio.sleep(1)

        if all_failed_ids:
            # Chạy lại đợt 2 cho các ID lỗi
            tasks = [fetch_product(session, pid, semaphore) for pid in all_failed_ids]
            r_results = await asyncio.gather(*tasks)
            r_success = [
                res["data"]
                for res in r_results
                if res and res.get("status") == "SUCCESS"
            ]
            if r_success:
                with open(
                    "tiki_products_retry_success.json", "w", encoding="utf-8"
                ) as f:
                    json.dump(r_success, f, ensure_ascii=False, indent=4)

    final_consolidation(all_ids)


if __name__ == "__main__":
    asyncio.run(main())
