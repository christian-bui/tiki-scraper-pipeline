# Project Handover: Tiki.vn Data Extraction

**Project Goal:** To extract 200,000 specific product profiles from Tiki.vn and save the clean data in JSON format. 

This document explains the system architecture, the recovery mechanisms, the data quality control process, and the step-by-step instructions to run the code.

## 1. Core System Features & Technical Decisions

- Data Freshness (API v2 Protocol): The system targets Tiki's V2 API endpoints. This ensures the extracted data reflects the latest product states, prices, and descriptions.
- High Speed (Concurrency): The system uses asynchronous processing (`aiohttp` and `asyncio`) to download multiple products simultaneously.
- Auto-Save & Deduplication: Successful IDs are logged into `success_ids.txt`. If the system restarts, it reads this file to resume progress without downloading the same product twice.
- Memory Optimization: Data is buffered in RAM and written to the disk only after collecting 1,000 products. This keeps the system stable and protects the hard drive.

## 2. Anti-Block & Auto-Recovery Mechanisms

Scraping 200,000 items requires a resilient system. This project implements a 4-Layer Defense System:

- Layer 1: Built-in Micro Retries (Inside Core Engine)
  If a request fails due to a minor network glitch, the system automatically retries that specific link up to 3 times immediately.
- Layer 2: Auto-Restart Process (`PM2`)
  If the script crashes due to memory issues or internet loss, PM2 will detect the failure and restart the script within 5 seconds.
- Layer 3: Standard Batch Retry (`retry_standard.py`)
  Gathers all IDs that failed Layer 1 and runs a secondary batch process to rescue them using standard network settings.
- Layer 4: Firewall Bypass (`retry_aiohttp.py`)
  If the IP is blocked (Error 403/429), this script drops tracking cookies and resets TCP connections to bypass the firewall for the remaining stubborn IDs.

## 3. Data Quality & Audit Report

A. Input Data Sanitization
The original list (`products-0-200000.csv`) was cleaned to remove hidden spaces and formatting errors before processing.

B. Local Data Audit (`audit_data.py`)
This script scans all output folders, counts successful unique records, and calculates exactly which IDs are missing compared to the original 200,000 list.

C. Web Verification (`verify_404.py`)
The audit identified approximately 18,010 missing IDs. We used this script to check their live links (e.g., https://tiki.vn/product-p{id}.html).
- Result: 100% of these IDs returned "404 Not Found" or redirected to the homepage.
- Conclusion: These products are officially dead or discontinued. The final JSON dataset represents 100% of the active products available on the platform.

## 4. Project Folder Structure

TIKI_SCRAPER/
├── `products-0-200000.csv`      # Original sanitized list of IDs
├── `main.py`                    # Main execution script
├── `config.py`                  # Global settings
├── `pipeline.log`               # Auto-generated execution logs
├── src/                         # Core source code
│   ├── `tiki.py`                # Main scraping logic
│   └── `utils.py`               # Data cleaning functions
└── scripts/                     # Helper tools
    ├── retries/
    │   ├── `retry_standard.py`  # Phase 2 recovery
    │   └── `retry_aiohttp.py`   # Phase 3 recovery (WAF bypass)
    ├── `audit_data.py`          # Phase 4: Data integrity scanner
    └── `verify_404.py`          # Phase 5: Dead link verification

## 5. How to Run the System (Step-by-Step)

Step A: Setup Environment
`python3 -m venv env`
`source env/bin/activate`
`pip install aiohttp`
`sudo npm install -g pm2`

Step B: Execution Order
1. Start Main Scraper:
   `pm2 start main.py --name "tiki-scraper" --interpreter ./env/bin/python3`
2. Run Standard Retry:
   `python3 scripts/retries/retry_standard.py`
3. Run Deep Retry (WAF Bypass):
   `python3 scripts/retries/retry_aiohttp.py`
4. Audit the Final Data:
   `python3 scripts/audit_data.py`
5. Verify Missing IDs:
   `python3 scripts/verify_404.py`

## 6. Output Data Format

JSON files are located in: `output/raw/api_v2/`

Each product contains:
- `id`: Unique product ID.
- `name`: Cleaned title.
- `url_key`: Web identifier.
- `price`: Price (Integer).
- `description`: Cleaned text (No HTML).
- `images_url`: List of image links.