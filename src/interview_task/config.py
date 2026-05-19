from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

INPUT_ORDERS_PATH = INPUT_DIR / "orders_events.jsonl"
INPUT_CUSTOMERS_PATH = INPUT_DIR / "customers.json"
OUTPUT_PATH = OUTPUT_DIR / "curated_orders"
