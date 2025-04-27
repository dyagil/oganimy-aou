from fastapi import FastAPI, Request, BackgroundTasks
import requests
import os
import uvicorn
import json
import time
import traceback
import hashlib
import urllib.parse
from datetime import datetime, timedelta
from dateutil import parser
from dotenv import load_dotenv

# טען משתני סביבה מקובץ .env (אם קיים)
load_dotenv()

app = FastAPI()

JOTFORM_URL = "https://form.jotform.com/202432710986455"
PIPEDRIVE_API_KEY = os.getenv("PIPEDRIVE_API_KEY", "TO_BE_REPLACED")  # יוחלף ב-Railway כ-ENV
BITLY_ACCESS_TOKEN = os.getenv("BITLY_ACCESS_TOKEN", "b77d50a5804d68a3762d38bad84749be9b1b0fc2")
JOTFORM_API_KEY = os.getenv("JOTFORM_API_KEY", "TO_BE_REPLACED")  # יש להוסיף מפתח API של JotForm

# מיפוי בין סוגי עסקאות לשאלונים ספציפיים
DEAL_TYPE_TO_FORM = {
    "החזר מס": {
        "form_id": "222503111662038",  # מזהה שאלון המס הנכון
        "form_name": "שאלון להחזר מס"
    },
    # ניתן להוסיף עוד סוגי עסקאות ושאלונים כאן
}

class ActivitiesManager:
    def __init__(self):
        pass

# שמירת מזהה עבור משימות שכבר נוצרו כדי למנוע כפילויות
task_history = {}

# פונקציה שבודקת אם כבר יצרנו משימה לאיש קשר ספציפי לאחרונה
def is_recent_task(person_id, field_value):
    task_key = f"{person_id}:{field_value}"
    current_time = datetime.now()
    
    # בדיקה אם כבר יצרנו משימה זהה לאחרונה (ב-30 שניות האחרונות)
    if task_key in task_history:
        last_time = task_history[task_key]
        # אם עברו פחות מ-30 שניות מהפעם האחרונה שיצרנו משימה זהה
        if current_time - last_time < timedelta(seconds=30):
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
        
        # השתמש ב-ID של איש הקשר כקוד הלקוח
        client_code = person_id  # ID איש הקשר בפייפדרייב
        
        print(f"Extracted person details: {first_name} {last_name}, phone: {phone}, email: {email}, client_code: {client_code}")
        
        # יצירת קישור לטופס JotForm עם שמות פרמטרים נכונים
        # הבטחת שכל הערכים הם מחרוזות או ריקים
        first_name = str(first_name) if first_name is not None else ""
        last_name = str(last_name) if last_name is not None else ""
        phone = str(phone) if phone is not None else ""
        email = str(email) if email is not None else ""
        client_code = str(client_code) if client_code is not None else ""
        
        jotform_link = f"{JOTFORM_URL}?name={urllib.parse.quote(first_name)}&Lname={urllib.parse.quote(last_name)}&phone={urllib.parse.quote(phone)}&email={urllib.parse.quote(email)}&typeA9={client_code}"
        print(f"Original JotForm link: {jotform_link}")
        
        # קיצור הקישור באמצעות Bitly
        try:
            bitly_url = "https://api-ssl.bitly.com/v4/shorten"
            payload = {
                "long_url": jotform_link
            }
            headers = {
                "Authorization": f"Bearer {BITLY_ACCESS_TOKEN}",
                "Content-Type": "application/json"
            }
            
            bitly_response = requests.post(bitly_url, json=payload, headers=headers, timeout=10)
            
            if bitly_response.status_code == 200 or bitly_response.status_code == 201:
                shortened_url = bitly_response.json().get("link")
                print(f"Shortened URL with Bitly: {shortened_url}")
                jotform_link = shortened_url
            else:
                print(f"Failed to shorten URL with Bitly. Status code: {bitly_response.status_code}")
                print(f"Response: {bitly_response.text[:200]}")
        except Exception as e:
            print(f"Error shortening URL with Bitly: {str(e)}")
            # במקרה של שגיאה, נשתמש בקישור המקורי
        
        # הכנת המשימה - עם פורמט מתאים למשימות פייפדרייב החדשות
        # איתור עסקאות הקשורות לאיש הקשר
        deals_url = f"https://api.pipedrive.com/v1/persons/{person_id}/deals?status=open&api_token={PIPEDRIVE_API_KEY}"
        try:
            deals_response = requests.get(deals_url, timeout=10)
            if deals_response.status_code == 200:
                deals_data = deals_response.json().get("data", [])
                if deals_data and len(deals_data) > 0:
                    # השתמש בעסקה הראשונה שנמצאה
                    deal_id = deals_data[0].get("id")
                    print(f"Found related deal ID: {deal_id}")
                else:
                    deal_id = None
                    print("No related deals found for person")
            else:
                deal_id = None
                print(f"Failed to get deals: {deals_response.status_code}")
        except Exception as e:
            deal_id = None
            print(f"Error fetching deals: {e}")
            
        # הכנת המשימה עם רק השדות הנתמכים ב-API
        task_payload = {
            "subject": f"שאלון תחומים ל{first_name} {last_name}",
            "type": "task",
            "due_date": (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d'),
            "due_time": "12:00",
            "person_id": person_id,
            "note": f"קישור לשאלון תחומים: {jotform_link}",
        }
        
        # הוספת מזהה עסקה אם נמצא
        if deal_id:
            task_payload["deal_id"] = deal_id
        
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
        # בדיקת כפילות רק בזמן קצר
        if is_recent_task(person_id, field_value):
            print(f"Skipping duplicate task for person_id: {person_id} with field_value: {field_value}")
            return {"status": "skipped", "reason": "Duplicate task", "person_id": person_id}
        
        print(f"Scheduling background task for person_id: {person_id} with field_value: {field_value}")
        background_tasks.add_task(create_jotform_task, person_id, field_value)
        return {"status": "processing", "person_id": person_id}
    else:
        print(f"Missing required data - person_id: {person_id}, field_value: {field_value}")
        return {"status": "ignored", "reason": "Missing required data"}

# מנהל פעילויות
activities_manager = ActivitiesManager()


async def create_deal_form_activity(deal_id, deal_data):
    """
    יצירת פעילות בפייפדרייב עם קישור לשאלון מתאים בהתאם לסוג העסקה
    """
    try:
        print("\n=========== CREATING DEAL FORM ACTIVITY ===========")
        print(f"Processing deal_id: {deal_id}")
        print(f"Deal data keys: {', '.join(deal_data.keys() if isinstance(deal_data, dict) else [])}")
        
        # הדפסת פרטים חשובים מהעסקה לצורך דיבוג
        if isinstance(deal_data, dict):
            title = deal_data.get("title", "")
            pipeline_id = deal_data.get("pipeline_id")
            stage_id = deal_data.get("stage_id")
            person_id = deal_data.get("person_id")
            
            print(f"Deal Title: {title}")
            print(f"Pipeline ID: {pipeline_id}")
            print(f"Stage ID: {stage_id}")
            print(f"Person ID: {person_id}")
            
            # פתרון חלופי למציאת הכותרת
            if not title and deal_id:
                # אם אין כותרת, ננסה להשיג את העסקה ישירות מה-API
                try:
                    print(f"Fetching deal details from API for deal_id: {deal_id}")
                    deal_url = f"https://api.pipedrive.com/v1/deals/{deal_id}?api_token={PIPEDRIVE_API_KEY}"
                    response = requests.get(deal_url)
                    if response.status_code == 200:
                        deal_details = response.json().get("data", {})
                        title = deal_details.get("title", "")
                        pipeline_id = deal_details.get("pipeline_id")
                        stage_id = deal_details.get("stage_id")
                        person_id = deal_details.get("person_id")
                        print(f"Retrieved from API - Deal title: {title}, pipeline: {pipeline_id}, person: {person_id}")
                    else:
                        print(f"Failed to get deal details: {response.status_code} {response.text}")
                except Exception as e:
                    print(f"Error fetching deal details: {e}")
        else:
            print(f"WARNING: deal_data is not a dictionary: {type(deal_data)}")
            # נסיון להשיג מידע ישירות מה-API
            try:
                print(f"Fetching deal details from API for deal_id: {deal_id}")
                deal_url = f"https://api.pipedrive.com/v1/deals/{deal_id}?api_token={PIPEDRIVE_API_KEY}"
                response = requests.get(deal_url)
                if response.status_code == 200:
                    deal_data = response.json().get("data", {})
                    title = deal_data.get("title", "")
                    pipeline_id = deal_data.get("pipeline_id")
                    stage_id = deal_data.get("stage_id")
                    person_id = deal_data.get("person_id")
                    print(f"Retrieved from API - Deal title: {title}, pipeline: {pipeline_id}, person: {person_id}")
                else:
                    print(f"Failed to get deal details: {response.status_code} {response.text}")
                    return
            except Exception as e:
                print(f"Error fetching deal details: {e}")
                return
        
        # בדיקה אם הכותרת מתאימה לאחד השאלונים שלנו
        deal_type = None
        print(f"Checking if deal title '{title}' matches any of our form types...")
        print(f"Available form types: {', '.join(DEAL_TYPE_TO_FORM.keys())}")
        
        for deal_type_key in DEAL_TYPE_TO_FORM.keys():
            print(f"Checking if '{deal_type_key}' is in '{title}'")
            if deal_type_key in title:
                deal_type = deal_type_key
                print(f"MATCH FOUND! Deal type: {deal_type}")
                break
        
        # אם לא מצאנו סוג עסקה מתאים, נצא מהפונקציה
        if not deal_type:
            print(f"No matching form found for deal title: {title}")
            return
        
        # קבלת פרטי השאלון המתאים
        form_info = DEAL_TYPE_TO_FORM[deal_type]
        form_id = form_info["form_id"]
        form_name = form_info["form_name"]
        print(f"Using form: {form_name} with ID: {form_id}")
        
        # יצירת קישור לשאלון עם פרמטרים מותאמים
        # הבטחת שכל הערכים הם מחרוזות
        deal_id_str = str(deal_id) if deal_id is not None else ""
        person_id_str = str(person_id) if person_id is not None else ""
        
        jotform_url = f"https://form.jotform.com/{form_id}?dealId={deal_id_str}&personId={person_id_str}"
        print(f"Generated form URL: {jotform_url}")
        
        # בדיקה אם כבר יצרנו פעילות כזו לעסקה זו
        activity_key = f"deal_form_{deal_id}_{form_id}"
        if activity_key in task_history:
            print(f"Form link activity already exists for deal {deal_id} and form {form_id}")
            return
        
        # קבלת פרטי הלקוח מפייפדרייב
        person_name = "לקוח"
        if person_id:
            try:
                print(f"Fetching person details for person_id: {person_id}")
                person_url = f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PIPEDRIVE_API_KEY}"
                response = requests.get(person_url)
                if response.status_code == 200:
                    person_data = response.json().get("data", {})
                    person_name = person_data.get("name", "לקוח")
                    print(f"Retrieved person name: {person_name}")
                else:
                    print(f"Failed to get person details: {response.status_code}")
            except Exception as e:
                print(f"Error fetching person details: {e}")
        
        # יצירת פעילות חדשה בפייפדרייב
        activity_data = {
            "subject": f"שאלון {form_name} ל{person_name}",
            "type": "task",
            "due_date": (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d'),
            "due_time": "12:00",
            "deal_id": deal_id,
            "person_id": person_id,
            "note": f"קישור לשאלון {form_name}: {jotform_url}",
        }
        
        print(f"Creating activity with data: {json.dumps(activity_data)}")
        
        # שליחת הבקשה ליצירת פעילות
        activity_url = f"https://api.pipedrive.com/v1/activities?api_token={PIPEDRIVE_API_KEY}"
        print(f"Sending request to: {activity_url}")
        
        response = requests.post(activity_url, json=activity_data)
        print(f"Response status code: {response.status_code}")
        
        if response.status_code == 201:
            print(f"SUCCESS: Created form link activity for deal {deal_id} with form {form_name}")
            # שמירת מזהה הפעילות למניעת כפילויות
            task_history[activity_key] = response.json().get("data", {}).get("id")
        else:
            print(f"FAILED to create form link activity: {response.status_code}")
            print(f"Response: {response.text[:500]}")
    
    except Exception as e:
        print(f"ERROR creating form link activity: {e}")
        print(traceback.format_exc())


@app.post("/deal-webhook")
async def handle_deal_webhook(request: Request, background_tasks: BackgroundTasks):
    """נקודת קצה לקבלת התראות על יצירת עסקאות חדשות"""
    try:
        # קבלת מידע מהבקשה
        data = await request.json()
        
        # הדפסה מפורטת של הנתונים שהתקבלו
        print("================ DEAL WEBHOOK REQUEST =================")
        print(f"Webhook data received: {json.dumps(data, indent=2)[:500]}...")  # מדפיס את 500 התווים הראשונים
        print(f"Webhook data keys: {', '.join(data.keys())}")
        
        # בדיקת קיום נתונים בסיסיים
        if not data:
            return {"status": "error", "message": "No data received"}
        
        # אתחול משתנים בסיסיים
        deal_id = None
        deal_data = None
        
        # איתור מזהה העסקה - בדיקה מקיפה יותר
        if "data" in data:
            print(f"Data section keys: {', '.join(data['data'].keys() if isinstance(data['data'], dict) else [])}")
            if isinstance(data['data'], dict) and "id" in data["data"]:
                deal_id = data["data"]["id"]
                deal_data = data["data"]
                print(f"Found deal_id in data.id: {deal_id}")
                # הדפסת כותרת העסקה אם קיימת
                if "title" in deal_data:
                    print(f"Deal title: {deal_data['title']}")
        elif "current" in data and isinstance(data['current'], dict):
            print(f"Current section keys: {', '.join(data['current'].keys())}")
            if "id" in data["current"]:
                deal_id = data["current"]["id"]
                deal_data = data["current"]
                print(f"Found deal_id in current.id: {deal_id}")
                if "title" in deal_data:
                    print(f"Deal title: {deal_data['title']}")
        
        # אם עדיין לא מצאנו, ננסה לחפש בכל מיני מקומות
        if not deal_id:
            print("Trying alternative paths to find deal_id...")
            paths_to_try = [
                lambda d: d.get("meta", {}).get("id"),
                lambda d: d.get("meta", {}).get("entity_id"),
                lambda d: d.get("event", "").split(".")[1] if "." in d.get("event", "") else None,
                lambda d: d.get("current", {}).get("deal_id", {}).get("value") if isinstance(d.get("current"), dict) else None,
                lambda d: d.get("id")
            ]
            
            for i, path_func in enumerate(paths_to_try):
                try:
                    val = path_func(data)
                    if val:
                        deal_id = val
                        deal_data = data  # יש סיכוי שנצטרך להתאים את זה
                        print(f"Found deal_id via path {i+1}: {deal_id}")
                        break
                except Exception as e:
                    print(f"Error checking path {i+1}: {e}")
        
        # נסיון לאתר את סוג העסקה ולהדפיס שלבי ביניים
        if deal_id and deal_data:
            # כדי להבין איך לזהות את סוג העסקה
            deal_type = None
            title = deal_data.get("title", "") if isinstance(deal_data, dict) else ""
            if title:
                print(f"Checking if deal title '{title}' matches any known deal types...")
                for deal_type_key in DEAL_TYPE_TO_FORM.keys():
                    if deal_type_key in title:
                        deal_type = deal_type_key
                        print(f"Matched deal type: {deal_type}")
                        break
            if not deal_type:
                print(f"WARNING: No matching form found for deal title: {title}")
        
        # אם מצאנו את הנתונים הנדרשים - נפעיל את המשימה
        if deal_id and deal_data:
            # בדיקת כפילות רק בזמן קצר
            if is_recent_task(str(deal_id), "deal"):
                print(f"Skipping duplicate task for deal_id: {deal_id}")
                return {"status": "skipped", "reason": "Duplicate task", "deal_id": deal_id}
            
            print(f"Scheduling background task for deal_id: {deal_id}")
            background_tasks.add_task(create_deal_form_activity, deal_id, deal_data)
            return {"status": "processing", "deal_id": deal_id}
        else:
            print(f"ERROR: Missing required data - deal_id: {deal_id}, deal_data available: {deal_data is not None}")
            return {"status": "ignored", "reason": "Missing required data"}
    
    except Exception as e:
        print(f"ERROR in deal webhook handler: {str(e)}")
        print(traceback.format_exc())
        return {"status": "error", "message": f"Internal error: {str(e)}"}

@app.on_event("startup")
async def on_startup():
    print("Application started")

@app.get("/")
async def root():
    return {"message": "Pipedrive-JotForm Bridge is running. Use /webhook endpoint for integration."}

@app.post("/clear-history")
async def clear_history():
    """נקה את היסטוריית המשימות כדי לאפשר יצירת משימות חדשות"""
    task_history.clear()
    return {"status": "success", "message": "Task history cleared"}

@app.post("/jotform-webhook")
async def handle_jotform_webhook(request: Request):
    """נקודת קצה לקבלת התראות מ-JotForm ואחזור התשובות דרך API"""
    try:
        # קבלת מידע מ-JotForm כ-form data
        form_data = await request.form()
        print("================ JOTFORM WEBHOOK REQUEST =================")
        print(f"JotForm data received with keys: {', '.join(form_data.keys())}")
        
        # לוג מפורט של כל השדות והערכים שלהם - יעזור לנו למפות שדות
        print("\nDetailed form data from JotForm webhook:")
        for key, value in form_data.items():
            print(f"  {key}: {value[:100] if isinstance(value, str) else value}")
        
        # בדיקת המידע שמתקבל מ-JotForm
        if not form_data:
            return {"status": "error", "message": "No data received from JotForm"}
            
        # מידע ההגשה
        submission_id = form_data.get("submissionID")
        
        if not submission_id:
            return {"status": "error", "message": "Submission ID not found in webhook data"}
            
        print(f"Received webhook for submission ID: {submission_id}")
        
        # שליפת מידע מלא על ההגשה דרך ה-API של JotForm
        submission_data = await get_jotform_submission(submission_id)
        
        if not submission_data:
            return {"status": "error", "message": "Failed to retrieve submission data"}
            
        # לוג מפורט של כל השדות שהתקבלו מה-API של JotForm
        print("\nDetailed submission data from JotForm API:")
        for key, value in submission_data.items():
            if not key.startswith('_'):  # להתעלם ממטה-דאטה
                shortened_value = str(value)[:100] + ('...' if len(str(value)) > 100 else '')
                print(f"  {key}: {shortened_value}")
            
        # חילוץ מזהה הלקוח מהנתונים
        client_id = None
        if "typeA9" in submission_data and submission_data["typeA9"]:
            client_id = submission_data["typeA9"]
            print(f"Found client ID: {client_id}")
        else:
            print("Client ID not found in form data. Checking all fields:")
            for field, value in submission_data.items():
                print(f"Field: {field}, Value: {value}")
            return {"status": "error", "message": "Client ID not found in submission data"}
            
        # עדכון כרטיס הלקוח בפייפדרייב
        success = await update_pipedrive_person(client_id, submission_data)
        
        if success:
            return {"status": "success", "message": "Person updated in Pipedrive"}
        else:
            return {"status": "error", "message": "Failed to update person in Pipedrive"}
            
    except Exception as e:
        print(f"ERROR in JotForm webhook handler: {str(e)}")
        print(traceback.format_exc())
        return {"status": "error", "message": f"Internal error: {str(e)}"}

async def get_jotform_submission(submission_id):
    """קבלת נתוני הגשה שלמים מ-JotForm API"""
    try:
        # API לקבלת נתוני טופס
        api_url = f"https://api.jotform.com/submission/{submission_id}?apiKey={JOTFORM_API_KEY}"
        print(f"Fetching submission data from: {api_url}")
        print(f"Using JotForm API key: {JOTFORM_API_KEY[:5]}...{JOTFORM_API_KEY[-5:] if JOTFORM_API_KEY else 'Not Set'}")
        
        # ניסיון עם מספר ניסיונות חוזרים
        for attempt in range(3):
            try:
                response = requests.get(api_url, timeout=10)
                
                if response.status_code == 200:
                    submission_json = response.json()
                    if submission_json.get("responseCode") == 200 and "content" in submission_json:
                        # הפיכת התשובה למבנה נוח יותר
                        answers = submission_json["content"].get("answers", {})
                        result = {}
                        metadata = {}
                        
                        # שמירת המידע על כותרות השדות לשימוש בפתק
                        metadata["field_labels"] = {}
                        
                        # עיבוד התשובות לפורמט נוח
                        for question_id, answer_data in answers.items():
                            # שמירת שם השדה הטכני
                            field_name = answer_data.get("name", question_id)
                            
                            # ניסיון לקבל כותרת אמיתית של השדה
                            field_label = None
                            if "text" in answer_data and answer_data["text"]:
                                field_label = answer_data["text"]
                            elif "sublabels" in answer_data and answer_data["sublabels"]:
                                field_label = next(iter(answer_data["sublabels"].values()), None)
                            elif "label" in answer_data and answer_data["label"]:
                                field_label = answer_data["label"]
                            
                            # שמירת כותרת השדה במטא-דאטה
                            if field_label and field_label.strip():
                                metadata["field_labels"][field_name] = field_label
                            
                            # שמירת התשובה עצמה
                            if "answer" in answer_data:
                                # בדיקה אם התשובה היא JSON
                                if isinstance(answer_data["answer"], dict):
                                    # ניסיון לחלץ מבנה ידוע (full - שימושי למספרי טלפון)
                                    if "full" in answer_data["answer"]:
                                        result[field_name] = answer_data["answer"]["full"]
                                    else:
                                        # חיבור הערכים לשרשרת אחת
                                        result[field_name] = ", ".join([str(v) for v in answer_data["answer"].values()])
                                else:
                                    result[field_name] = answer_data["answer"]
                            elif "text" in answer_data:
                                result[field_name] = answer_data["text"]
                        
                        print(f"Processed submission with {len(result)} fields and {len(metadata['field_labels'])} field labels")
                        
                        # הוספת המטא-דאטה לתוצאה
                        result["_metadata"] = metadata
                        return result
                        
                    else:
                        print(f"Invalid response format from JotForm API: {submission_json}")
                else:
                    print(f"Failed attempt {attempt+1} with status code {response.status_code}")
                    print(f"Response: {response.text[:200]}")
                    
                time.sleep(1)  # המתן לפני ניסיון נוסף
                    
            except Exception as e:
                print(f"Exception in attempt {attempt+1}: {str(e)}")
                time.sleep(1)
        
        print("Failed to retrieve submission data after 3 attempts")
        return None
        
    except Exception as e:
        print(f"ERROR in get_jotform_submission: {str(e)}")
        print(traceback.format_exc())
        return None

async def update_pipedrive_fields(person_id, form_data):
    """עדכון שדות של כרטיס לקוח בפייפדרייב על סמך תשובות מג'וטפורם"""
    try:
        print(f"\n========= ATTEMPTING TO UPDATE PIPEDRIVE FIELDS ==========")
        print(f"Person ID: {person_id}")
        
        # מיפוי בין שדות ג'וטפורם לשדות פייפדרייב
        # מפתח: שדה ג'וטפורם, ערך: מזהה שדה פייפדרייב
        field_mapping = {
            # מיפויים קודמים
            "input18": "aef7138242c2a32ca51ec09c35df1bfa4c756f2c",    # מין
            "input109": "298a5a71694995d831cd85c12084b71714234057",  # מספר תעודת זהות
            
            # נתונים אישיים
            "input117": "ab7c49cd143665a08d4f4d24fcd33a5597c003fd",  # תאריך לידה
            "input118": "62c775c3816aa805892280fad530d42bc1813512",  # מין
            "input107": "e54db6d7f2d66f2b568ab5debf077fa27622bf38",  # מצב משפחתי
            
            # נדל"ן ומשכנתאות
            "input89": "b73a179abc7a7ff81990f7691924b31347fdd4b5",    # האם ישנה דירה בבעלותך
            "input90": "2a6a352fc718a85b7df174532adca479532c0a47",    # האם ישנה משכנתה פעילה
            "input91": "fe4c82280b5bda5ca50af7ddd355b3f638b0cb13",   # מעוניין משכנתא הפוכה
            "input92": "b343cdaf8cc8af635ec387f899948511479523ff",   # מעוניין בדירה
            
            # ייעוץ פיננסי וביטוח
            "input66": "d6a67420629cc21c0273961284a50350a7b035c3",   # האם מעוניין בייעוץ פיננסי פנסיוני
            "input67": "cd5d573be92a9a2b0a1ed80f9748ec3735d00435",   # האם מעוניין בבדיקה מול ביטוח לאומי
            "input93": "7c6ede34a86356f946d8aa9cf740303fedc6cad1",   # מעוניין ביעוץ פרישה
            "input94": "d6a67420629cc21c0273961284a50350a7b035c3",   # מעוניין בבדיקת ביטוחים
            "input96": "f04b812c4af0a8f11cafabdcf2af452188cbb593",   # מעוניין בבדיקה מול ביטוח לאומי
            
            # שונות
            "input123": "1c3a70dc4012b04cf5def563aff73b47b3005982"   # האם הנך נוסע לחו"ל בחצי שנה הקרובה
        }
        
        # הדפסת נתוני השדות הרלוונטיים שיש לנו בנתונים
        for jotform_field in field_mapping.keys():
            if jotform_field in form_data:
                print(f"Found field {jotform_field} in form data with value: {form_data[jotform_field]}")
            else:
                print(f"Field {jotform_field} not found in form data")
        
        # בדיקה אם יש שדות לעדכון
        fields_to_update = {}
        for jotform_field, pipedrive_field in field_mapping.items():
            if jotform_field in form_data and form_data[jotform_field]:
                # בדיקה שיש ערך בשדה ושהוא לא ריק
                
                # טיפול מיוחד בשדה תאריך לידה
                if jotform_field == "input117" and form_data[jotform_field]:
                    try:
                        # הדפסת הערך המקורי לצורך דיבוג
                        original_date = form_data[jotform_field]
                        print(f"Original date value from JotForm: {original_date}")
                        
                        # הסרת רווחים ותווים שאינם חלק מהתאריך
                        cleaned_date = original_date.strip()
                        print(f"Cleaned date: {cleaned_date}")
                        
                        # ננסה להשתמש ב-dateutil parser שהוא הכי גמיש
                        try:
                            # ניסיון עם פורמט אמריקאי (MM/DD/YYYY)
                            parsed_date = parser.parse(cleaned_date, dayfirst=False)
                            formatted_date = parsed_date.strftime("%Y-%m-%d")
                            print(f"Parsed date as MM/DD/YYYY: {formatted_date}")
                        except Exception:
                            try:
                                # ניסיון עם פורמט אירופאי (DD/MM/YYYY)
                                parsed_date = parser.parse(cleaned_date, dayfirst=True)
                                formatted_date = parsed_date.strftime("%Y-%m-%d")
                                print(f"Parsed date as DD/MM/YYYY: {formatted_date}")
                            except Exception as e:
                                print(f"Failed to parse date: {e}")
                                # אם כל הניסיונות נכשלו, נשתמש בערך המקורי
                                formatted_date = original_date
                        
                        # עדכון השדה בפייפדרייב
                        fields_to_update[pipedrive_field] = formatted_date
                        print(f"Final date format for Pipedrive: {formatted_date}")
                    
                    except Exception as e:
                        print(f"Error in date processing: {e}")
                        # נשאיר את התאריך המקורי במקרה של שגיאה
                        fields_to_update[pipedrive_field] = form_data[jotform_field]
                else:
                    # טיפול רגיל לשאר השדות
                    fields_to_update[pipedrive_field] = form_data[jotform_field]
        
        print(f"Fields to update: {fields_to_update}")
        
        if not fields_to_update:
            print("No fields to update in Pipedrive")
            return True  # מחזירים הצלחה כי אין שגיאה, פשוט אין מה לעדכן
        
        # עדכון השדות בפייפדרייב
        api_url = f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PIPEDRIVE_API_KEY}"
        print(f"Sending PUT request to {api_url}")
        print(f"With data: {json.dumps(fields_to_update)}")
        
        # ניסיון עם ריטריי לעדכן את השדות
        for attempt in range(3):
            try:
                response = requests.put(api_url, json=fields_to_update, timeout=10)
                
                print(f"Attempt {attempt+1} response status: {response.status_code}")
                print(f"Response content: {response.text[:200]}")
                
                if response.status_code == 200:
                    response_data = response.json()
                    if response_data.get("success"):
                        print(f"Successfully updated {len(fields_to_update)} fields for person {person_id}")
                        return True
                
                print(f"Failed to update fields in Pipedrive. Status code: {response.status_code}")
                
            except requests.exceptions.RequestException as e:
                print(f"Request error when updating Pipedrive fields: {e}")
            
            # המתנה לפני ניסיון נוסף
            if attempt < 2:  # לא נמתין אחרי הניסיון האחרון
                time.sleep(1)
        
        return False
        
    except Exception as e:
        print(f"Error updating Pipedrive fields: {e}")
        print(traceback.format_exc())
        return False

async def update_pipedrive_person(person_id, form_data):
    """יצירת פתק עם תשובות השאלון והצמדתו לכרטיס הלקוח בפייפדרייב"""
    try:
        # יצירת תוכן הפתק מכל התשובות בשאלון - עיצוב חדש
        current_date = datetime.now().strftime("%d/%m/%Y %H:%M")
        note_content = f"# תשובות שאלון {current_date}\n\n"
        
        # קבלת המידע על כותרות השדות
        field_labels = {}
        if "_metadata" in form_data and "field_labels" in form_data["_metadata"]:
            field_labels = form_data["_metadata"]["field_labels"]
        
        # רשימת שדות להתעלם מהם לחלוטין
        ignored_fields = [
            "typeA9", "typeA8", "date", "ip", "date124", "form_name", 
            "submission_id", "control_text", "control_text_2", "control_6", "control_1",
            "website", "submit", "submission_id", "view", "anchor1", "anchor2", "anchor3", "anchor4",
            "submit_form", "preferred_time", "signature", "form_title", "form_id"
        ]
        
        # מילות מפתח לשדות שלא רוצים להציג
        ignored_texts = [
            "עוגנים", "anchor", "אנכור", 
            "תודה שמילאת", 
            "אישורי", "הערות", 
            "הוראות", "חתימה"
        ]
        
        # ערכים שיחשבו כריקים
        empty_values = ["", "null", "undefined", None, "0", "-", "N/A", "n/a", "לא", 
                       "תאריך", "מיקוד", "תעודת", "חתימה"]
        
        # מיון שדות לקטגוריות
        categories = {
            "פרטים אישיים": [],  # פרטים אישיים
            "התעניינות": [],              # התעניינות
            "תשובות נוספות": []  # תשובות נוספות
        }
        
        # זיהוי שדות שהשם שלהם זהה לערך (כמו שאלות כן/לא בג'וטפורם)
        def clean_value(value):
            """ ניקוי הערך מתווים מיוחדים להשוואה """
            if not value:
                return ""
            return str(value).lower().strip().replace("?", "").replace(":", "")
        
        # שדות שיופיעו בקטגוריה פרטים אישיים
        personal_fields = ['name', 'Lname', 'phone', 'email', 'typeA7', 'typeA10']
        # שדות שיופיעו בקטגוריה התעניינות
        interests_fields = ['typeA23', 'typeA11', 'typeA24', 'typeA48']
        
        # עיבוד השדות לקטגוריות
        items_added = 0
        
        for field_name, field_value in form_data.items():
            # התעלם משדות להתעלמות, שדות מערכת ושדות ריקים
            if (field_name not in ignored_fields and 
                not field_name.startswith("_") and 
                field_value not in empty_values and 
                str(field_value).strip() != ""):
                
                # ניסיון להשיג כותרת שדה אם קיימת
                if field_name in field_labels and field_labels[field_name].strip():
                    # שימוש בכותרת השדה האמיתית מה-API
                    field_label = field_labels[field_name]
                else:
                    # ניסיון להשיג כותרת משם השדה
                    field_label = field_name
                    if "_" in field_name:
                        field_label = field_name.split("_")[-1]
                    
                    # טיפול בשדות מיוחדים
                    if field_name.startswith("input"):
                        field_label = "שאלה " + field_name.replace("input", "")
                    elif field_name.startswith("typeA"):
                        field_label = "שדה " + field_name.replace("typeA", "")
                
                # בדיקה אם השדה או הערך מכיל טקסט שרוצים להתעלם ממנו
                should_ignore = False
                for ignored_text in ignored_texts:
                    if (ignored_text.lower() in str(field_label).lower() or 
                        ignored_text.lower() in str(field_value).lower()):
                        should_ignore = True
                        break
                
                # בדיקה אם כותרת השדה והערך שלו זהים או דומים מאד (כמו בשאלות כן/לא של ג'וטפורם)
                clean_label = clean_value(field_label)
                clean_value_str = clean_value(field_value)
                
                if clean_label and clean_value_str and (clean_label == clean_value_str or 
                   (len(clean_label) > 10 and clean_label in clean_value_str) or
                   (len(clean_value_str) > 10 and clean_value_str in clean_label)):
                    # אם הם זהים או מכילים אחד את השני - נשנה את הפורמט
                    if clean_value_str == "כן" or clean_value_str == "yes" or clean_value_str == "true":
                        # אם זו שאלת כן/לא שנענתה ב"\u05db\u05df" - נשאיר רק את כותרת השדה בלי הערך
                        field_value = "✓"  # סימון של "\u05db\u05df"
                    elif clean_value_str == "לא" or clean_value_str == "no" or clean_value_str == "false":
                        field_value = "✗"  # סימון של "\u05dc\u05d0"
                    else:
                        # כותרת וערך זהים שאינם כן/לא - נשאיר רק את כותרת השדה
                        field_value = "✓"  # נשתמש בסימון להראות שהלקוח בחר זאת
                
                if should_ignore:
                    continue
                
                # הכנת הערך המעוצב לתצוגה בצורה אסתטית יותר - ללא כוכביות
                if field_value == "✓":
                    # אם זו שאלת כן/לא או ערך זהה לשם השדה - נציג רק את שם השדה עם סימון ✓
                    formatted_field = f"○ {field_label} ✓"
                elif field_value == "✗":
                    # אם זו שאלת כן/לא שסומנה כ'לא' - נציג את שם השדה עם סימון ✗
                    formatted_field = f"○ {field_label} ✗"
                else:
                    # אחרת - נציג את שם השדה והערך
                    formatted_field = f"○ {field_label}: {field_value}"
                
                # הוספה לקטגוריה המתאימה
                if field_name in personal_fields or any(name in field_name for name in ['name', 'phone', 'mail']):
                    categories["פרטים אישיים"].append(formatted_field)
                elif field_name in interests_fields or any(name in field_label.lower() for name in ['עניין', 'מעוניין', 'מתעניין', 'רוצה']):
                    categories["התעניינות"].append(formatted_field)
                else:
                    categories["תשובות נוספות"].append(formatted_field)
                
                items_added += 1
        
        # הסרת כפילויות בכל קטגוריה
        for category in categories:
            # יצירת מילון שיחליף רשימה וימנע כפילויות
            unique_fields = {}
            for field in categories[category]:
                # חילוץ כותרת השדה (החלק אחרי הסימון העגול)
                field_parts = field.split("○ ")
                if len(field_parts) >= 2:
                    # אם יש תו ✓ או ✗ - הכותרת היא החלק שלפני התו
                    field_title = field_parts[1].split(" ✓")[0].split(" ✗")[0].split(":")[0].strip()
                    # אם כבר קיים שדה עם אותה כותרת, נחליף אותו רק אם החדש קצר יותר
                    if field_title not in unique_fields or len(field) < len(unique_fields[field_title]):
                        unique_fields[field_title] = field
            
            # החלפת הרשימה המקורית במילון ללא כפילויות
            categories[category] = list(unique_fields.values())
        
        # הוספת הקטגוריות לתוכן הפתק עם עיצוב חדש
        for category, fields in categories.items():
            if fields:  # רק אם יש שדות בקטגוריה
                note_content += f"## {category}\n\n"
                for field in fields:
                    note_content += f"{field}\n\n"
                note_content += "---\n\n"  # מפריד בין קטגוריות
        
        print(f"Prepared note content with {items_added} non-empty fields organized into categories")
        
        # יצירת הפתק בפייפדרייב
        # הסרת הסימון האחרון של מפריד בסוף הפתק
        if note_content.endswith("---\n\n"):
            note_content = note_content[:-5]
        
        # הוספת סיכום
        if items_added > 0:
            note_content += f"\n\nסהכ: {items_added} שדות מידע נאספו בשאלון."
        
        note_payload = {
            "content": note_content,
            "person_id": person_id,
            "pinned_to_person_flag": True
        }
        
        # ה-API ליצירת פתקים בפייפדרייב
        notes_api_url = f"https://api.pipedrive.com/v1/notes?api_token={PIPEDRIVE_API_KEY}"
        
        # ניסיון עם מספר ניסיונות חוזרים
        for attempt in range(3):
            try:
                print(f"Attempt {attempt+1}: Creating note in Pipedrive for person ID: {person_id}")
                
                # שימוש ב-POST ליצירת פתק חדש
                response = requests.post(notes_api_url, json=note_payload, timeout=10)
                
                if response.status_code == 200 or response.status_code == 201:
                    print(f"SUCCESS: Created note for Pipedrive person {person_id} with form data")
                    print(f"Response: {response.text[:200]}...")
        
                    # עדכון שדות פייפדרייב (אם יש צורך)
                    # קריאה לפונקציה החדשה לעדכון שדות
                    fields_result = await update_pipedrive_fields(person_id, form_data)
                    if not fields_result:
                        print("Warning: Fields were not updated in Pipedrive, but note was created successfully")
        
                    return True
                else:
                    print(f"Failed attempt {attempt+1} with status code {response.status_code}")
                    print(f"Response: {response.text[:200]}")
                    time.sleep(1)  # המתן לפני ניסיון נוסף
            except Exception as e:
                print(f"Exception in attempt {attempt+1}: {str(e)}")
                time.sleep(1)
        
        print(f"ERROR: Failed to create note in Pipedrive after 3 attempts")
        return False
        
    except Exception as e:
        print(f"ERROR in update_pipedrive_person: {str(e)}")
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
