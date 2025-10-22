import httpx
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, List

BARBASE_API = "https://bb.solutionary.me/api/v1"
# API_KEY = os.getenv("BARBASE_API_KEY", "YOUR_SECRET_TOKEN")
API_KEY = "YOUR_API_KEY"
STATE_FILE = "sync_state_v1.json"
FOODREPO_FILE = "foodrepo_data.json"

HEADERS = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}


# === HELPERS ===
def load_json(filename: str) -> Any:
    if not os.path.exists(filename):
        return {}
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(filename: str, data: Any):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


async def check_barbase_product(client: httpx.AsyncClient, barcode: str) -> httpx.Response:
    """Проверяем, есть ли товар в Barbase."""
    url = f"{BARBASE_API}/barcodes/{barcode}"
    return await client.get(url, headers=HEADERS, params={"page": 0, "per_page": 0})


async def create_barbase_product(client: httpx.AsyncClient, product: dict) -> bool:
    """Создаём новый продукт (если его нет в базе)."""
    body = {
        "barcode": product["barcode"],
        "external_user_id": None,
        "images": product.get("image_links", []),
        "language_id": None,
        "name": product["name"]
    }
    resp = await client.post(f"{BARBASE_API}/products", headers=HEADERS, json=body)
    if resp.status_code == 201:
        print(f"[+] Created new product {product['name']} ({product['barcode']})")
        return True
    else:
        print(f"[ERROR {resp.status_code}] Failed to create {product['barcode']}: {resp.text}")
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
        print(f"[+] Added name '{name}' to product {product_id}")
    else:
        print(f"[WARN] Could not add name ({resp.status_code})")


async def add_barbase_images(client: httpx.AsyncClient, product_id: int, images: List[str]):
    for url in images:
        body = {
            "external_user_id": None,
            "product_id": product_id,
            "url": url
        }
        resp = await client.post(f"{BARBASE_API}/images", headers=HEADERS, json=body)
        if resp.status_code == 201:
            print(f"[+] Added image {url} to product {product_id}")
        else:
            print(f"[WARN] Could not add image ({resp.status_code})")


# === MAIN SYNC ===
async def sync_products():
    """Основной процесс: сверяет FoodRepo и Barbase."""
    state = load_json(STATE_FILE)
    updated_state = state.copy()

    # Загружаем все продукты из FoodRepo
    if not os.path.exists(FOODREPO_FILE):
        print("[ERROR] File foodrepo_data.json not found — run parser first!")
        return

    food_products = load_json(FOODREPO_FILE)
    print(f"[INFO] Loaded {len(food_products)} products from FoodRepo")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for product in food_products:
            barcode = product["barcode"]

            # игнорируем некорректные
            if barcode in ("", "Not found", None):
                continue

            created_at = product["created_at"]
            updated_at = product["updated_at"]

            prev = state.get(barcode)
            if prev and prev["updated_at"] == updated_at:
                print(f"[=] {barcode}: already synced, skipping.")
                continue

            # Проверяем наличие продукта в Barbase
            resp = await check_barbase_product(client, barcode)

            if resp.status_code == 200:
                data = resp.json()
                product_id = data["data"][0]["products"][0]["id"]

                # добавляем имя и картинки (дополняем!)
                await add_barbase_name(client, product_id, product["name"])
                await add_barbase_images(client, product_id, product.get("image_links", []))
                status = "updated"

            elif resp.status_code == 404:
                # создаём новый
                await create_barbase_product(client, product)
                status = "uploaded"

            else:
                print(f"[ERROR] Unexpected response {resp.status_code} for {barcode}")
                continue

            updated_state[barcode] = {
                "barcode": barcode,
                "status": status,
                "last_posted": datetime.now(timezone.utc).isoformat(),
                "created_at": created_at,
                "updated_at": updated_at
            }

    save_json(STATE_FILE, updated_state)
    print("\n[✔] Sync complete!")


if __name__ == "__main__":
    asyncio.run(sync_products())
