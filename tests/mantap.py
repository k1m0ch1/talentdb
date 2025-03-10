import time
import requests
import notion
from requests.auth import HTTPBasicAuth

start_time = time.time()

#TALENTDB_URL = "https://stein.efishery.com/v1/storages/67a76631989e3e625bb6d2c5"
TALENTDB_URL = "https://stein.efishery.com/v1/storages/67a76dc7989e3e3547b6d2c7"
#TALENTDB_URL = "https://api.steinhq.com/v1/storages/67ba18e8c08833336578a661"
TALENTDB_SHEETNAME = "db"
TALENTDB_USER = "k1m0ch1"
TALENTDB_PASS = "gelutlahjeungainganjing"
NOTION_APIKEY = "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"
NOTION_APIURL = "https://api.notion.com/v1"
NOTION_PRIVATEDB_ID = "19fd9265013680c3ba51f9a46c2db19c"
NOTION_PUBLICDB_ID = "1a2d92650136806a9b99eb50bee96e98"
NOTION_HEADERS = {
        "Authorization": f"Bearer {NOTION_APIKEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
        }
basic = HTTPBasicAuth(TALENTDB_USER, TALENTDB_PASS)
payload = { 
    #"limit": 3,
    "offset": 0 
}

TARGET_NOTIONDB = [NOTION_PRIVATEDB_ID, NOTION_PUBLICDB_ID]


r = requests.get(f"{TALENTDB_URL}/{TALENTDB_SHEETNAME}",auth=basic, params=payload)

if r.status_code == 200:
    data = r.json()
    for item in data:
        # skip if didn't need to show
        if item["is Show"] != "Eligible" or item["is Hired"] == "TRUE":
            print(f"[?] Record {item['Name']} isn't eligible or already hired")
            continue

        # now check if the employee is already existed
        talent_name = item["Name"]
        if not talent_name:
            continue

        search_payload = {
            "filter": {
                "property": "Name",
                "title": {"equals": talent_name}
            }
        }

        for NOTION_DBID in TARGET_NOTIONDB:

            data_payload = {}

            search_req = requests.post(f"{NOTION_APIURL}/databases/{NOTION_DBID}/query",
                                    headers=NOTION_HEADERS, json=search_payload)
            
            NEWDATA = False

            if search_req.status_code == 200:
                search_results = search_req.json()["results"]
                if search_results:
                    # if employee exist, just update the Record
                    existing_page_id = search_results[0]["id"]
                    
                    data_payload = {
                        "properties": {}
                    }
                else:
                    # if employee does not exist, create new record
                    data_payload = {
                        "parent": {"database_id": NOTION_DBID},
                        "properties":{ }
                    }
                    NEWDATA = True

            getProp = requests.get(f"{NOTION_APIURL}/databases/{NOTION_DBID}", headers=NOTION_HEADERS)

            properties = {}

            if getProp.status_code == 200:
                propData = getProp.json()
                dbName = propData.get("title", [])
                if dbName:
                    dbName = dbName[0].get("text", {}).get("content", "Unkown Database")

                # just to be pretty, print here to know the status update or new record
                if NEWDATA:
                    print(f"[+][{dbName}-{NOTION_DBID}] Record {item['Name']} is not exist, proceed to create a new record")
                else:
                    print(f"[+][{dbName}-{NOTION_DBID}] Record {item['Name']} is already exist, proceed to update record")

                properties = propData.get("properties", {})
                for field_name, details in properties.items():
                    # print(f"{field_name}: {details.get('type')}")
                    if item[field_name] != None:
                        if details.get('type') == "title":
                            data_payload["properties"][field_name] = notion.addTitle(item[field_name])
                        elif details.get('type') == "url":
                            data_payload["properties"][field_name] = notion.addURL(item[field_name])
                        elif details.get('type') == "phone_number":
                            data_payload["properties"][field_name] = notion.addPhone(item[field_name])
                        elif details.get('type') == "email":
                            data_payload["properties"][field_name] = notion.addEmail(item[field_name])
                        elif details.get('type') == "select":
                            data_payload["properties"][field_name] = notion.addSelect(item[field_name])
                        elif details.get('type') == "multi_select":
                            data_payload["properties"][field_name] = notion.addMultiSelect(item[field_name])
                        elif details.get('type') == "date":
                            data_payload["properties"][field_name] = notion.addDate(item[field_name])
                        elif details.get('type') == "rich_text":
                            data_payload["properties"][field_name] = notion.addRichText(item[field_name])
                        else:
                            break

            if NEWDATA:
                reqData = requests.post(f"{NOTION_APIURL}/pages", headers=NOTION_HEADERS, json=data_payload)
                # print(reqData.status_code)
                if reqData.status_code != 200:
                    print(reqData.text)
            else:
                updateData = requests.patch(f"{NOTION_APIURL}/pages/{existing_page_id}", headers=NOTION_HEADERS, json=data_payload)
                if updateData.status_code != 200:
                    print(updateData.text)

            # pause a second for easy rate limit handler
            time.sleep(1)
else:

    print(r.status_code)

print(f"-- Script Executed {(time.time() - start_time)} seconds --")
