

def addTitle(value):
    return {
        "title":[
            {"text": {"content": value}}
        ]
    }

def addSelect(value):
    return {
        "select": {
            "name": value
        }
    }

def addMultiSelect(value):
    options = [{"name": opt.strip()} for opt in value.split(", ") if opt.strip()]
    return {
        "multi_select": options        
    }

def addDate(value):
    return {
        "date": { "start": value if value != None else None }
    }

def addRichText(value):
    return {
        "rich_text": [
            {"text": {"content": value}}
        ]
    }

def addURL(value):
    return {"url": value}

def addEmail(value):
    return {"email": value}

def addPhone(value):
    return {"phone_number": value}

def addCheckBox(value):
    if value == 'FALSE':
        value = False
    if value == 'TRUE':
        value = True 
    if value == None:
        value = False
    return {"checkbox": value }

def readValue(value):
    if value['type'] == "url":
        return value["url"]
    if value['type'] == "phone_number":
        return value["phone_number"]
    if value['type'] == "rich_text":
        return value["rich_text"][0]["plain_text"]
    if value["type"] == "select":
        return value["select"]["name"]
    if value["type"] == "multi_select":
        return ", ".join(item["name"] for item in value["multi_select"])
    if value["type"] == "email":
        return value["email"]
    if value["type"] == "title":
        return value["title"][0]["plain_text"]
    return ""


