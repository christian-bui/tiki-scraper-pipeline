"""
Configuration settings for the Tiki data pipeline.
Centralized control for performance, rate limiting, and storage paths.
"""

from pathlib import Path

# --- 1. EXECUTION CONTROLS ---
# Set LIMIT to None for a full production run, or an integer (e.g., 50) for testing.
LIMIT = None

# --- 2. PERFORMANCE & WAF EVASION (RATE LIMITING) ---
# Checkpoint frequency: Save data to disk after every 500 IDs.
CHUNK_SIZE = 500

# Concurrency: Maximum number of simultaneous async requests.
SEMAPHORE_LIMIT = 25

# Network: Max wait time for a single API response before raising a TimeoutError.
TIMEOUT_SECONDS = 30

# Stealth Mode: Pause in seconds between chunks to avoid triggering WAF blocks.
BATCH_DELAY = 3

# --- 3. PATHS & STORAGE ---
INPUT_FILE = "products-0-200000.csv"
OUTPUT_ROOT = Path("output/raw")

# --- 4. API ENDPOINTS ---
API_VERSION = "v2"
API_BASE_URL = f"https://api.tiki.vn/product-detail/api/{API_VERSION}/products"
