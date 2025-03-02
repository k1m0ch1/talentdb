import asyncio
import aiohttp
import hashlib
import json
import time 
import requests
import notion
import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

start_time = time.time()

load_dotenv()

TALENTDB_DB_URL = os.getenv("TALENTDB_DB_URL")
TALENTDB_DB_SHEETNAME = os.getenv("TALENTDB_DB_SHEETNAME")
TALENTDB_DB_USER = os.getenv("TALENTDB_DB_USER")
TALENTDB_DB_PASS = os.getenv("TALENTDB_DB_PASS")
TALENTDB_NOTION_APIKEY = os.getenv("TALENTDB_NOTION_APIKEY")
TALENTDB_NOTION_APIURL = os.getenv("TALENTDB_NOTION_APIURL")
TALENTDB_NOTION_DB_ID = os.getenv("TALENTDB_NOTION_DB_ID")

NOTION_HEADERS = {
        "Authorization": f"Bearer {TALENTDB_NOTION_APIKEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
        }
basic = HTTPBasicAuth(TALENTDB_DB_USER, TALENTDB_DB_PASS)

payload = { 
    #"limit": 3,
    "offset": 0 
}

NOTION_PAGES_PROPERTIES = {}

# Get the default database properties
def get_database_properties():
    result = {}
    url = f"{TALENTDB_NOTION_APIURL}/databases/{TALENTDB_NOTION_DB_ID}"
    response = requests.get(url, headers=NOTION_HEADERS)

    if response.status_code == 200:
        database_info = response.json()
        properties = database_info.get("properties", {})

        result = {}

        for prop_name, prop_details in properties.items():
            result[prop_name] = prop_details

        return result
    else:
        print(f"Failed to fetch database properties: {response.text}")
        return None


# Fetch all notion record
def fetch_all_notion_records():

    print(f"Fetch Database")
    notion_records = {}

    query_url = f"{TALENTDB_NOTION_APIURL}/databases/{TALENTDB_NOTION_DB_ID}/query"
    payload = {}

    while True:
        response = requests.post(query_url, headers=NOTION_HEADERS, json=payload)
        if response.status_code != 200:
            print(f"Error fetching NOtion DB {db_id}: {response.text}")
            break

        data = response.json()
        for result in data.get("results", []):
            page_id = result['id']
            properties = result.get("properties", {})
            name_property = properties["Name"]["title"]

            if name_property:
                name = name_property[0]["text"]["content"]
                notion_records[name] = {
                    "id": page_id,
                    "properties": properties
                }

        if not data["has_more"]:
            break
        payload["start_cursor"] = data["next_cursor"]

    return notion_records

def compute_hash(data):
    json_string = json.dumps(data, sort_keys=True)
    return hashlib.sha256(json_string.encode()).hexdigest()

# need some attention rate limit API notion is three request per second
# and also they have some kind of size limit of every data type
# ref: https://developers.notion.com/reference/request-limits
async def notion_record(session, notion_pages, talent, page_properties):
    talent_name = talent.get("Name")

    if not talent_name:
        return

    talent_data = {k: v for k, v in talent.items() if k not in ["is Show", "is Hired"]}
    new_hash = compute_hash(talent_data)

    # Check if talent already exists in Notion 
    if talent_name in notion_pages:
        existing_page = notion_pages[talent_name]
        existing_hash = compute_hash(existing_page["properties"])

        if existing_hash == new_hash:
            print(f"Skipping unchanged record: {talent_name}")
            return

        # skipping because incomplete
        if talent['NIK'] == '#N/A':
            print(f"Skipping {talent_name} because not exist in master data employee")
            return
        if talent['Job Specialization'] in ["", "#N/A" ,None] or talent['Skills'] in ["", "#N/A", None]:
            print(f"Skipping {talent_name} because incomplete Profile")
            return
        if talent['is Show'] != "Eligible":
            print(f"Skipping {talent_name} because Show status is {talent['is Show']}")
            return
        if talent['is Hired'] == "TRUE":
            print(f"Skipping {talent_name} because already Hired")
            return
        
        update_payload = {"properties": {}}
        
        for field_name, detail in page_properties.items():
            update_payload["properties"][field_name] = notion.setValue(detail["type"], talent[field_name])

        async with session.patch(f"{TALENTDB_NOTION_APIURL}/pages/{existing_page['id']}",
                                 headers=NOTION_HEADERS, json=update_payload) as response:
            if response.status == 200:
                print(f"Updated {talent_name}")
            else:
                print(f"Failed to update {talent_name}: {await response.text()}")
                import pdb; pdb.set_trace()
    else:
        create_payload = {
            "parent": {"database_id": TALENTDB_NOTION_DB_ID},
            "properties": {}
        }

        for field_name, detail in page_properties.items():
            create_payload["properties"][field_name] = notion.setValue(detail['type'], talent[field_name])

        # Send async request to create the record
        async with session.post(f"{TALENTDB_NOTION_APIURL}/pages", headers=NOTION_HEADERS, json=create_payload) as response:
            if response.status == 200:
                print(f"Created {talent_name}")
            else:
                print(f"Failed to create {talent_name}: {await response.text()}")

async def main():
    NOTION_PAGES_PROPERTIES = get_database_properties()
    notion_pages = fetch_all_notion_records()
    response = requests.get(f"{TALENTDB_DB_URL}/{TALENTDB_DB_SHEETNAME}", auth=basic)
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
            print(f"Check Data {talent['Name']}")
            tasks.append(notion_record(session, notion_pages, talent, NOTION_PAGES_PROPERTIES))

        await asyncio.gather(*tasks)

asyncio.run(main())

print(f"-- Script Executed in {(time.time() - start_time)} seconds --")
        
