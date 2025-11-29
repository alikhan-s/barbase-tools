import asyncio
import httpx
import json
import os
from datetime import datetime

STATE_FILE = "state.json"
OUTPUT_FILE = "products.json"

API_URL = "https://world.openfoodfacts.org/api/v2/search"

CONCURRENCY = 10          # параллельные запросы
PAGE_SIZE = 100           # чем больше — тем быстрее, но риск ошибок выше
STOP_IF_EMPTY = True      # если API вернул пустую страницу — стоп


# -----------------------------
#  STATE MANAGER
# -----------------------------
def load_state():
    if not os.path.exists(STATE_FILE):
        return {"last_updated_t": 0, "page": 1}

    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# -----------------------------
#  JSON STREAM WRITER
# -----------------------------
def ensure_output_file():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w") as f:
            f.write("[\n")


def append_product(product):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False)
        f.write(",\n")


# -----------------------------
#  API REQUEST
# -----------------------------
async def fetch_page(client: httpx.AsyncClient, page: int, last_updated_t: int):
    params = {
        "page": page,
        "page_size": PAGE_SIZE,
        "fields": "code,product_name,image_url,last_updated_t",
        "sort_by": "last_updated_t",
        "sort_order": "asc",
        "last_updated_t": f">{last_updated_t}"
    }

    r = await client.get(API_URL, params=params)

    if r.status_code != 200:
        print(f"[ERROR] page {page} status {r.status_code}")
        return None

    return r.json()


# -----------------------------
#  EXTRACT PRODUCT
# -----------------------------
def extract_product(p):
    return {
        "barcode": p.get("code"),
        "name": p.get("product_name"),
        "image_links": [p.get("image_url")],
        "updated_at": datetime.utcfromtimestamp(p["last_updated_t"]).isoformat()
    }


# -----------------------------
#  WORKER
# -----------------------------
async def worker(page_queue: asyncio.Queue, state):
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            page = await page_queue.get()
            if page is None:
                return

            data = await fetch_page(client, page, state["last_updated_t"])

            if not data or "products" not in data:
                page_queue.task_done()
                continue

            products = data["products"]

            if STOP_IF_EMPTY and len(products) == 0:
                print("Reached empty page → stopping early.")
                page_queue.task_done()
                return

            latest_ts = state["last_updated_t"]

            for p in products:
                if "last_updated_t" not in p:
                    continue

                ts = p["last_updated_t"]
                if ts > latest_ts:
                    latest_ts = ts

                append_product(extract_product(p))

            state["last_updated_t"] = latest_ts
            state["page"] = page
            save_state(state)

            print(f"[PAGE {page}] processed {len(products)} products")

            page_queue.task_done()


# -----------------------------
#  MAIN LOOP
# -----------------------------
async def main():
    state = load_state()
    ensure_output_file()

    start_page = state["page"]
    print(f"Resuming from page {start_page}, last_updated_t={state['last_updated_t']}")

    page_queue = asyncio.Queue()

    for page in range(start_page, 10_000_000):
        page_queue.put_nowait(page)

    tasks = []
    for _ in range(CONCURRENCY):
        t = asyncio.create_task(worker(page_queue, state))
        tasks.append(t)

    await page_queue.join()

    for _ in range(CONCURRENCY):
        page_queue.put_nowait(None)

    await asyncio.gather(*tasks)

    print("DONE. Close JSON manually with ] when finished parsing.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped manually.")
