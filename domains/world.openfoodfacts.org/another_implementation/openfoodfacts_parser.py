import httpx
import asyncio
import json

async def main():
    url = "https://world.openfoodfacts.org/api/v2/search"

    params = {
        "page": 1,
        "page_size": 10,
        "fields": "code,product_name,image_url,created_t,last_modified_t,last_updated_t",
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=params)
        print("STATUS:", response.status_code)
        print(json.dumps(response.json(), indent=2))

asyncio.run(main())
