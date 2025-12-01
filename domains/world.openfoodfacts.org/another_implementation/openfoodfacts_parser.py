import asyncio
import httpx
import json
import os
from datetime import datetime

STATE_FILE = "state.json"
OUTPUT_FILE = "products.json"

API_URL = "https://world.openfoodfacts.org/api/v2/search"

CONCURRENCY = 20
PAGE_SIZE = 100
MAX_PAGES_PER_CYCLE = 200


# -----------------------------
# STATE
# -----------------------------
def load_state():
    if not os.path.exists(STATE_FILE):
        return {"last_updated_t": 0}

    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# -----------------------------
# OUTPUT
# -----------------------------
def ensure_output_file():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w") as f:
            f.write("[\n")


def append_product(p):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False)
        f.write(",\n")


# -----------------------------
# API REQUEST
# -----------------------------
async def fetch_page(client: httpx.AsyncClient, page: int, last_updated_t: int, retries=3):
    params = {
        "page": page,
        "page_size": PAGE_SIZE,
        "fields": "code,product_name,image_url,last_updated_t",
        "sort_by": "last_updated_t",
        "sort_order": "asc",
        "last_updated_t": f">{last_updated_t}"
    }

    for attempt in range(retries):
        try:
            r = await client.get(API_URL, params=params)
            if r.status_code == 200:
                return r.json()
            return None

        except (httpx.ReadTimeout, httpx.ConnectTimeout):
            print(f"[TIMEOUT] page {page}, attempt {attempt+1}/{retries}")
            await asyncio.sleep(1 + attempt)

    print(f"[FAILED] page {page}")
    return None


# -----------------------------
# PRODUCT CLEANER
# -----------------------------
def extract_product(p):
    return {
        "barcode": p.get("code"),
        "name": p.get("product_name"),
        "image_links": [p.get("image_url")],
        "updated_at": datetime.utcfromtimestamp(p["last_updated_t"]).isoformat()
    }


# -----------------------------
# FETCH MANY PAGES IN PARALLEL
# -----------------------------
async def fetch_cycle(last_updated_t):
    async with httpx.AsyncClient(timeout=60.0) as client:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def wrapped(page):
            async with sem:
                return await fetch_page(client, page, last_updated_t)

        tasks = [asyncio.create_task(wrapped(page)) for page in range(1, MAX_PAGES_PER_CYCLE + 1)]
        return await asyncio.gather(*tasks)


# -----------------------------
# MAIN LOOP
# -----------------------------
async def main():
    state = load_state()
    ensure_output_file()

    print(f"Starting with last_updated_t={state['last_updated_t']}")

    while True:
        batch = await fetch_cycle(state["last_updated_t"])

        all_products = []
        max_ts = state["last_updated_t"]
        empty_pages = 0

        for idx, data in enumerate(batch, start=1):
            if not data or "products" not in data:
                empty_pages += 1
                continue

            products = data["products"]

            if len(products) == 0:
                empty_pages += 1
                continue

            print(f"[PAGE {idx}] +{len(products)} products")

            for p in products:
                if "last_updated_t" not in p:
                    continue

                ts = p["last_updated_t"]
                if ts > max_ts:
                    max_ts = ts

                all_products.append(extract_product(p))

        # no new data -> stop
        if empty_pages == MAX_PAGES_PER_CYCLE:
            print("No more new data. Stopping.")
            break

        # write downloaded products
        for pr in all_products:
            append_product(pr)

        print(f"Downloaded {len(all_products)} new products in this cycle.")
        print(f"Updated last_updated_t → {max_ts}")

        state["last_updated_t"] = max_ts
        save_state(state)

    print("Parsing finished. Don't forget to close array with ]")


if __name__ == "__main__":
    asyncio.run(main())
