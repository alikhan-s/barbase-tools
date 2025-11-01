import httpx
import asyncio
import json
import os
import logging
import random
import re
from datetime import datetime, timezone
from typing import Any, List

# === CONFIG ===
BARBASE_API = "https://bb.solutionary.me/api/v1"
API_KEY = "d9997c62-6b0c-4f61-9c7e-decae5d968a3"
STATE_FILE = "data/sync_state_v2.json"
FOODREPO_FILE = "data/foodrepo_data.json"

HEADERS = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}

MAX_CONCURRENT = 10        # Number of parallel tasks
SAVE_EVERY = 10_000        # Save progress every N entries
RETRIES = 5                # Attempts in case of network/API error

# === LOGGING SETUP ===
LOG_FILE = "data/sync.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# === HELPERS ===
def load_json(filename: str) -> Any:
    if not os.path.exists(filename):
        return {}
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(filename: str, data: Any):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


async def with_retries(func, *args, retries=RETRIES, base_delay=1.0, max_delay=10.0, **kwargs):
    """Repeats the function with an exponential delay in case of an error."""
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            delay += random.uniform(0, 0.5)
            log.warning(f"{func.__name__}: attempt {attempt}/{retries} failed ({e}). Retrying in {delay:.1f}s...")
            await asyncio.sleep(delay)
    log.error(f"{func.__name__}: all {retries} attempts failed.")
    return None


# === API CALLS ===
async def check_barbase_product(client: httpx.AsyncClient, barcode: str) -> httpx.Response:
    return await client.get(f"{BARBASE_API}/barcodes/{barcode}", headers=HEADERS)


async def create_barbase_product(client: httpx.AsyncClient, product: dict) -> bool:
    body = {
        "barcode": product["barcode"],
        "external_user_id": None,
        "images": product.get("image_links") or ["https://barbase.solutionary.me/empty.jpg"],
        "language_id": None,
        "name": product["name"]
    }
    resp = await client.post(f"{BARBASE_API}/products", headers=HEADERS, json=body)
    if resp.status_code == 201:
        log.info(f"[+] Created new product {product['name']} ({product['barcode']})")
        return True
    log.error(f"[{resp.status_code}] Failed to create {product['barcode']}: {resp.text}")
    return False


async def add_barbase_name(client: httpx.AsyncClient, product_id: int, name: str):
    body = {
        "external_user_id": None,
        "language_id": None,
        "name": name,
        "product_id": product_id
    }
    resp = await client.post(f"{BARBASE_API}/names", headers=HEADERS, json=body)
    if resp.status_code == 201:
        log.info(f"[+] Added name '{name}' to product {product_id}")
    else:
        log.warning(f"[WARN] Could not add name ({resp.status_code})")


async def add_barbase_images(client: httpx.AsyncClient, product_id: int, images: List[str]):
    if images:
        for url in images:
            body = {"external_user_id": None, "product_id": product_id, "url": url}
            resp = await client.post(f"{BARBASE_API}/images", headers=HEADERS, json=body)
            if resp.status_code == 201:
                log.info(f"[+] Added image {url} to product {product_id}")
            else:
                log.warning(f"[WARN] Could not add image ({resp.status_code})")
    else:
        log.warning(f"[WARN] Could not find images for ({product_id})")


# === CORE ===
SEM = asyncio.Semaphore(MAX_CONCURRENT)

async def process_product(client, product, state, updated_state):
    async with SEM:
        barcode = str(product.get("barcode", "")).strip()

        if not barcode or barcode.lower() in ("not found", "none"):
            return

        # Clean barcodes of debris
        barcode = re.sub(r'[^\x20-\x7E]', '', str(barcode)).strip()

        # Skip incorrect barcodes
        if not barcode.isdigit() or len(barcode) < 5:
            log.warning(f"Skipping invalid barcode: {barcode!r}")
            return

        created_at = product.get("created_at")
        updated_at = product.get("updated_at")

        prev = state.get(barcode)
        if prev and (prev.get("updated_at") == updated_at or prev.get("created_at") == created_at):
            log.debug(f"{barcode}: already synced, skipping.")
            return

        resp = await with_retries(check_barbase_product, client, barcode)
        if not resp:
            log.error(f"[ERROR] Failed to check {barcode} after retries.")
            return

        if resp.status_code == 200:
            data = resp.json()
            product_id = data["data"][0]["products"][0]["id"]
            await with_retries(add_barbase_name, client, product_id, product["name"])
            await with_retries(add_barbase_images, client, product_id, product.get("image_links", []))
            status = "updated"

        elif resp.status_code == 404:
            await with_retries(create_barbase_product, client, product)
            status = "uploaded"

        else:
            log.error(f"[ERROR] Unexpected response {resp.status_code} for {barcode}")
            return

        updated_state[barcode] = {
            "barcode": barcode,
            "status": status,
            "last_posted": datetime.now(timezone.utc).isoformat(),
            "created_at": created_at,
            "updated_at": updated_at
        }

        if len(updated_state) % SAVE_EVERY == 0:
            save_json(STATE_FILE, updated_state)
            log.info(f"[STATE] Progress saved ({len(updated_state)} records)")


async def sync_products():
    state = load_json(STATE_FILE)
    updated_state = state.copy()

    if not os.path.exists(FOODREPO_FILE):
        log.error("[ERROR] File foodrepo_data.json not found — run parser first!")
        return

    food_products = load_json(FOODREPO_FILE)
    log.info(f"[INFO] Loaded {len(food_products)} products from FoodRepo")

    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = []
        for idx, product in enumerate(food_products, start=1):
            task = process_product(client, product, state, updated_state)
            tasks.append(task)

        await asyncio.gather(*tasks)

        save_json(STATE_FILE, updated_state)
        log.info("[STATE] Final state saved")
    log.info("[✔] Sync complete!")


if __name__ == "__main__":
    try:
        asyncio.run(sync_products())
    except KeyboardInterrupt:
        log.warning("Interrupted by user. Saving state...")
