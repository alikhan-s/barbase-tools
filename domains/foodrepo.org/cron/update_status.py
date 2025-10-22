import json

STATE_FILE = "sync_state_v1.json"
FOODREPO_FILE = "foodrepo_data.json"

STOP_BARCODE = "7613312084182"

def save_partial_state():
    with open(FOODREPO_FILE, "r", encoding="utf-8") as f:
        food_products = json.load(f)

    state = {}
    for product in food_products:
        barcode = product.get("barcode")
        if not barcode:
            continue

        state[barcode] = {
            "barcode": barcode,
            "status": "partial_saved",
            "created_at": product.get("created_at"),
            "updated_at": product.get("updated_at")
        }

        # если дошли до нужного barcode — выходим
        if barcode == STOP_BARCODE:
            print(f"[INFO] Reached barcode {STOP_BARCODE}, stopping save.")
            break

    # сохраняем в sync_state_v1.json
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

    print(f"[✔] Saved partial state up to barcode {STOP_BARCODE} ({len(state)} items).")


if __name__ == "__main__":
    save_partial_state()
