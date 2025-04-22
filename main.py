from fastapi import FastAPI, Request, BackgroundTasks
import requests
import os
import uvicorn
import json
import time
import traceback
import hashlib
from datetime import datetime, timedelta
from dotenv import load_dotenv
import urllib.parse

# טען משתני סביבה מקובץ .env (אם קיים)
load_dotenv()

app = FastAPI()

JOTFORM_URL = "https://form.jotform.com/202432710986455"
PIPEDRIVE_API_KEY = os.getenv("PIPEDRIVE_API_KEY", "TO_BE_REPLACED")  # יוחלף ב-Railway כ-ENV

# שמירת מזהה עבור משימות שכבר נוצרו כדי למנוע כפילויות
task_history = {}

# פונקציה שבודקת אם כבר יצרנו משימה לאיש קשר ספציפי לאחרונה
def is_recent_task(person_id, field_value):
    task_key = f"{person_id}:{field_value}"
    current_time = datetime.now()
    
    # בדיקה אם כבר יצרנו משימה זהה לאחרונה (ב-2 דקות האחרונות)
    if task_key in task_history:
        last_time = task_history[task_key]
        # אם עברו פחות מ-2 דקות מהפעם האחרונה שיצרנו משימה זהה
        if current_time - last_time < timedelta(minutes=2):
            print(f"Duplicate task detected for {task_key}. Last created at {last_time}")
            return True
    
    # עדכון זמן היצירה של המשימה הנוכחית
    task_history[task_key] = current_time
    return False

# פונקציה שתרוץ ברקע ליצירת משימה ב-Pipedrive
def create_jotform_task(person_id, field_value):
    try:
        print(f"Starting background task for person_id: {person_id}")
        
        # שליפת פרטי איש הקשר מ-Pipedrive
        api_url = f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PIPEDRIVE_API_KEY}"
        
        # ניסיון עם מספר ניסיונות חוזרים
        for attempt in range(3):
            try:
                print(f"Attempt {attempt+1}: Fetching person data from {api_url}")
                person_res = requests.get(api_url, timeout=10)  # מגדיר timeout של 10 שניות
                
                if person_res.status_code == 200:
                    print(f"Success! Got response with status code {person_res.status_code}")
                    break
                    
                print(f"Failed attempt {attempt+1} with status code {person_res.status_code}")
                # המתן לפני ניסיון נוסף
                time.sleep(1)
            except Exception as e:
                print(f"Exception in attempt {attempt+1}: {str(e)}")
                time.sleep(1)
        
        if person_res.status_code != 200:
            print(f"ERROR: Failed to fetch person data after 3 attempts. Status: {person_res.status_code}")
            print(f"Response: {person_res.text[:200]}")
            return
            
        person_data = person_res.json()
        if "data" not in person_data or not person_data["data"]:
            print(f"ERROR: Could not retrieve person data. Response: {person_res.text[:200]}")
            return
            
        person = person_data.get("data", {})
        
        # חילוץ פרטי איש הקשר בצורה בטוחה
        first_name = person.get("first_name", "")
        last_name = person.get("last_name", "")
        
        phone = ""
        if person.get("phone") and len(person.get("phone", [])) > 0:
            phone = person.get("phone", [{}])[0].get("value", "")
        
        email = ""
        if person.get("email") and len(person.get("email", [])) > 0:
            email = person.get("email", [{}])[0].get("value", "")
        
        print(f"Extracted person details: {first_name} {last_name}, phone: {phone}, email: {email}")
        
        # יצירת קישור לטופס JotForm
        jotform_link = f"{JOTFORM_URL}?first_name={urllib.parse.quote(first_name)}&last_name={urllib.parse.quote(last_name)}&phone={urllib.parse.quote(phone)}&email={urllib.parse.quote(email)}"
        print(f"Original JotForm link: {jotform_link}")
        
        # קיצור הקישור באמצעות TinyURL
        try:
            tinyurl_api = f"https://tinyurl.com/api-create.php?url={urllib.parse.quote(jotform_link)}"
            shortened_url_response = requests.get(tinyurl_api, timeout=10)
            
            if shortened_url_response.status_code == 200:
                shortened_url = shortened_url_response.text.strip()
                print(f"Shortened URL: {shortened_url}")
                jotform_link = shortened_url
            else:
                print(f"Failed to shorten URL. Status code: {shortened_url_response.status_code}")
        except Exception as e:
            print(f"Error shortening URL: {str(e)}")
            # במקרה של שגיאה, נשתמש בקישור המקורי
        
        # הכנת המשימה
        task_payload = {
            "subject": "לחיצה על הקישור תוביל לשאלון תחומים",
            "done": 0,
            "person_id": person_id,
            "note": f"היי! הנה הקישור שלך לטופס:\n{jotform_link}",
        }
        
        # יצירת משימה חדשה ב-Pipedrive
        task_url = f"https://api.pipedrive.com/v1/activities?api_token={PIPEDRIVE_API_KEY}"
        
        # ניסיון עם מספר ניסיונות חוזרים
        for attempt in range(3):
            try:
                print(f"Attempt {attempt+1}: Creating task at {task_url}")
                task_res = requests.post(task_url, json=task_payload, timeout=10)
                
                if task_res.status_code in [200, 201]:
                    print(f"SUCCESS: Created task for person {person_id} with link: {jotform_link}")
                    print(f"Task creation response: {task_res.text[:200]}")
                    return
                
                print(f"Failed attempt {attempt+1} with status code {task_res.status_code}")
                # המתן לפני ניסיון נוסף
                time.sleep(1)
            except Exception as e:
                print(f"Exception in attempt {attempt+1}: {str(e)}")
                time.sleep(1)
        
        print(f"ERROR: Failed to create task after 3 attempts")
        
    except Exception as e:
        print(f"ERROR in background task: {str(e)}")
        print(traceback.format_exc())

@app.post("/webhook")
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    # קבל את המידע מהבקשה
    data = await request.json()
    
    # הדפסה בסיסית של הנתונים שהתקבלו
    print("================ WEBHOOK REQUEST =================")
    print(f"Webhook data received with keys: {', '.join(data.keys())}")
    
    # בדיקת קיום נתונים בסיסיים
    if not data:
        return {"status": "error", "message": "No data received"}
    
    # אתחול משתנים בסיסיים
    person_id = None
    field_value = None
    
    # איתור מזהה איש הקשר
    if "data" in data and "id" in data["data"]:
        person_id = data["data"]["id"]
        print(f"Found person_id in data.id: {person_id}")
    else:
        # לוגיקה חלופית לאיתור מזהה איש הקשר
        paths_to_try = [
            lambda d: d.get("current", {}).get("person_id", {}).get("value"),
            lambda d: d.get("id"),
            lambda d: d.get("current", {}).get("id"),
            lambda d: d.get("current", {}).get("id") if d.get("event") == "updated.person" else None,
            lambda d: d.get("previous", {}).get("id"),
            lambda d: d.get("meta", {}).get("entity_id")
        ]
        
        for i, path_func in enumerate(paths_to_try):
            try:
                val = path_func(data)
                if val:
                    person_id = val
                    print(f"Found person_id via path {i+1}: {person_id}")
                    break
            except Exception as e:
                print(f"Error checking path {i+1}: {e}")
    
    # איתור ערך השדה המיוחד
    target_field_id = "51b05f4fe90c769c81299ac0d2bad3e75a02903e"
    
    if ("data" in data and "custom_fields" in data["data"] and 
            target_field_id in data["data"]["custom_fields"]):
        custom_field = data["data"]["custom_fields"][target_field_id]
        if custom_field and isinstance(custom_field, dict) and "id" in custom_field:
            field_value = custom_field["id"]
            print(f"Found field in data.custom_fields: {field_value}")
    elif ("current" in data and "custom_fields" in data["current"] and 
            target_field_id in data["current"]["custom_fields"]):
        custom_field = data["current"]["custom_fields"][target_field_id]
        if custom_field and isinstance(custom_field, dict) and "id" in custom_field:
            field_value = custom_field["id"]
            print(f"Found field in current.custom_fields: {field_value}")
    elif ("custom_fields" in data and target_field_id in data["custom_fields"]):
        custom_field = data["custom_fields"][target_field_id]
        if custom_field and isinstance(custom_field, dict) and "id" in custom_field:
            field_value = custom_field["id"]
            print(f"Found field in root.custom_fields: {field_value}")
    
    # אם מצאנו את הנתונים הנדרשים - בדוק אם זו משימה כפולה
    if person_id and field_value:
        if is_recent_task(person_id, field_value):
            print(f"Skipping duplicate task for person_id: {person_id} with field_value: {field_value}")
            return {"status": "skipped", "reason": "Duplicate task", "person_id": person_id}
        
        print(f"Scheduling background task for person_id: {person_id} with field_value: {field_value}")
        background_tasks.add_task(create_jotform_task, person_id, field_value)
        return {"status": "processing", "person_id": person_id}
    else:
        print(f"Missing required data - person_id: {person_id}, field_value: {field_value}")
        return {"status": "ignored", "reason": "Missing required data"}

@app.get("/")
async def root():
    return {"message": "Pipedrive-JotForm Bridge is running. Use /webhook endpoint for integration."}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
