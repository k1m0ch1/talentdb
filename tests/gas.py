import asyncio
import aiohttp
import hashlib
import json
import time
import requests
from requests.auth import HTTPBasicAuth

start_time = time.time()

# Notion & TalentDB Configuration
TALENTDB_URL = "https://stein.efishery.com/v1/storages/67a76dc7989e3e3547b6d2c7"
TALENTDB_SHEETNAME = "db"
TALENTDB_USER = "k1m0ch1"
TALENTDB_PASS = "gelutlahjeungainganjing"
NOTION_APIKEY = "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"
NOTION_APIURL = "https://api.notion.com/v1"
TARGET_NOTIONDB = ["19fd9265013680c3ba51f9a46c2db19c", "1a2d92650136806a9b99eb50bee96e98"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_APIKEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

basic = HTTPBasicAuth(TALENTDB_USER, TALENTDB_PASS)


### 🔹 STEP 1: Fetch All Notion Records (Batch Fetch for Speed) ###
def fetch_all_notion_records():
    notion_records = {db_id: {} for db_id in TARGET_NOTIONDB}
    
    for db_id in TARGET_NOTIONDB:
        query_url = f"{NOTION_APIURL}/databases/{db_id}/query"
        payload = {}

        while True:
            response = requests.post(query_url, headers=NOTION_HEADERS, json=payload)
            if response.status_code != 200:
                print(f"Error fetching Notion DB {db_id}: {response.text}")
                break

            data = response.json()
            for result in data.get("results", []):
                page_id = result["id"]
                properties = result.get("properties", {})
                name_property = properties.get("Name", {}).get("title", [])

                if name_property:
                    name = name_property[0]["text"]["content"]
                    notion_records[db_id][name] = {
                        "id": page_id,
                        "properties": properties
                    }

            # Pagination Handling
            if not data.get("has_more"):
                break
            payload["start_cursor"] = data["next_cursor"]

    return notion_records


### 🔹 STEP 2: Compute Hash for Fast Change Detection ###
def compute_hash(data):
    """Creates a unique hash for the given data."""
    json_string = json.dumps(data, sort_keys=True)
    return hashlib.sha256(json_string.encode()).hexdigest()


### 🔹 STEP 3: Async Function to Update/Create Records in Notion ###
async def update_or_create_notion_record(session, notion_pages, talent, notion_db_id):
    talent_name = talent.get("Name")
    if not talent_name:
        return

    # Prepare the new data hash
    talent_data = {k: v for k, v in talent.items() if k not in ["is Show", "is Hired"]}
    new_hash = compute_hash(talent_data)

    # Check if talent already exists in Notion
    if talent_name in notion_pages[notion_db_id]:
        existing_page = notion_pages[notion_db_id][talent_name]
        existing_hash = compute_hash(existing_page["properties"])

        # Skip update if data is unchanged
        if existing_hash == new_hash:
            print(f"Skipping unchanged record: {talent_name}")
            return
        
        # Prepare update payload
        update_payload = {"properties": {}}
        for field_name, value in talent.items():
            if value:
                update_payload["properties"][field_name] = {"rich_text": [{"text": {"content": value}}]}

        # Send async request to update the record
        async with session.patch(f"{NOTION_APIURL}/pages/{existing_page['id']}",
                                 headers=NOTION_HEADERS, json=update_payload) as response:
            if response.status == 200:
                print(f"Updated {talent_name}")
            else:
                print(f"Failed to update {talent_name}: {await response.text()}")

    else:
        # Prepare create payload
        create_payload = {
            "parent": {"database_id": notion_db_id},
            "properties": {
                "Name": {"title": [{"text": {"content": talent_name}}]}
            }
        }
        for field_name, value in talent.items():
            if value:
                create_payload["properties"][field_name] = {"rich_text": [{"text": {"content": value}}]}

        # Send async request to create the record
        async with session.post(f"{NOTION_APIURL}/pages",
                                headers=NOTION_HEADERS, json=create_payload) as response:
            if response.status == 200:
                print(f"Created {talent_name}")
            else:
                print(f"Failed to create {talent_name}: {await response.text()}")


### 🔹 STEP 4: Main Async Function ###
async def main():
    # Fetch all Notion records in one batch request
    notion_pages = fetch_all_notion_records()
    
    # Fetch all TalentDB records in one batch request
    response = requests.get(f"{TALENTDB_URL}/{TALENTDB_SHEETNAME}", auth=basic)
    if response.status_code != 200:
        print(f"Failed to fetch TalentDB data: {response.status_code}")
        return
    
    talent_data = response.json()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for talent in talent_data:
            # Skip if not eligible
            if talent.get("is Show") != "Eligible" or talent.get("is Hired") == "TRUE":
                continue

            # Create tasks for async processing
            for notion_db_id in TARGET_NOTIONDB:
                tasks.append(update_or_create_notion_record(session, notion_pages, talent, notion_db_id))

        await asyncio.gather(*tasks)


### 🔹 STEP 5: Run Async Event Loop ###
asyncio.run(main())

print(f"-- Script Executed in {(time.time() - start_time)} seconds --")

