import asyncio
import aiohttp
import hashlib
import json
import time 
import requests
from dotenv import load_dotenv()
from requests.auth import HTTPBasicAuth

start_time = time.time()

lod_dotenv()

TALENTDB_DB_URL = os.getenv("TALENTDB_DB_URL")
TALENTDB_DB_SHEETNAME = os.getenv("TALENTDB_DB_SHEETNAME")
TALENTDB_DB_USER = os.getenv("TALENTDB_DB_USER")
TALENTDB_DB_PASS = os.getenv("TALENTDB_DB_PASS")
TALENTDB_NOTION_APIKEY = os.getenv("TALENTDB_NOTION_APIKEY")
TALENTDB_NOTION_APIURL = os.getenv("TALENTDB_NOTION_APIURL")
TALENTDB_NOTION_PRIVATEDB_ID = os.getenv("TALENTDB_NOTION_PRIVATEDB_ID")
TALENTDB_NOTION_PUBLICDB_ID = os.getenv("TALENTDB_NOTION_PUBLICDB_ID")

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

TARGET_NOTIONDB = [TALENTDB_NOTION_PRIVATEDB_ID, TALENTDB_NOTION_PUBLICDB_ID]


# Fetch all notion record
def fetch_all_notion_records():
    notion_records = {db_id: {} for db_id in TARGET_NOTIONDB}

    for db_id in TARGET_NOTIONDB:
        query_url = f"{TALENTDB_NOTION_APIURL}/databases/{db_id}/query"
        payload = {}
