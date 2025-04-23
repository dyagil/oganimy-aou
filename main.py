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
from dotenv import load_dotenv

# טען משתני סביבה מקובץ .env (אם קיים)
load_dotenv()

app = FastAPI()

JOTFORM_URL = "https://form.jotform.com/202432710986455"
PIPEDRIVE_API_KEY = os.getenv("PIPEDRIVE_API_KEY", "TO_BE_REPLACED")  # יוחלף ב-Railway כ-ENV
BITLY_ACCESS_TOKEN = os.getenv("BITLY_ACCESS_TOKEN", "b77d50a5804d68a3762d38bad84749be9b1b0fc2")
JOTFORM_API_KEY = os.getenv("JOTFORM_API_KEY", "TO_BE_REPLACED")  # יש להוסיף מפתח API של JotForm

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
    """טיפול באירועים מהווק של ג'וטפורם"""
    try:
        # קבלת הנתונים מהבקשה
        form_data = await request.form()
        
        # המרת נתוני הטופס למילון
        webhook_data = dict(form_data)
        print(f"Received JotForm webhook with {len(webhook_data)} fields")   
        
        # קבלת מזהה הגשת הטופס
        if 'submission_id' not in webhook_data:
            print("submission_id not found in webhook data")
            return {"success": False, "error": "Missing submission_id"}
            
        submission_id = webhook_data.get('submission_id')
        print(f"Processing submission ID: {submission_id}")
        
        # בדיקה שיש מזהה לקוח בפייפדרייב בנתונים
        if 'typeA9' not in webhook_data:
            print("Person ID (typeA9) not found in webhook data")
            return {"success": False, "error": "Missing person_id"}
            
        person_id = webhook_data.get('typeA9')
        print(f"Found person_id: {person_id}")
        
        # קבלת נתוני הגשת הטופס ממני ה-API של ג'וטפורם
        submission_data = await get_jotform_submission(submission_id)
        if not submission_data:
            print("Failed to fetch submission data")
            return {"success": False, "error": "Failed to fetch submission data"}
        
        # לוג של כל השדות שהתקבלו מג'וטפורם (לצורך דיבאג ומיפוי)
        for field_key in submission_data.keys():
            if not field_key.startswith('_'):
                print(f"Submission field: {field_key} = {submission_data[field_key]}")
            
        # עדכון הפתק בפייפדרייב עם תשובות השאלון
        result = await update_pipedrive_person(person_id, submission_data)
        
        if result:
            return {"status": "success", "message": "Note created and fields updated successfully"}
        else:
            return {"status": "error", "message": "Failed to process submission data"}
            
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
        # מיפוי בין שדות ג'וטפורם לשדות פייפדרייב
        # מפתח: שדה ג'וטפורם, ערך: מזהה שדה פייפדרייב
        field_mapping = {
            "input18": "aef7138242c2a32ca51ec09c35df1bfa4c756f2c"
            # הוסף כאן מיפויים נוספים
        }
        
        # בדיקה אם יש שדות לעדכון
        fields_to_update = {}
        for jotform_field, pipedrive_field in field_mapping.items():
            if jotform_field in form_data and form_data[jotform_field]:
                # בדיקה שיש ערך בשדה ושהוא לא ריק
                fields_to_update[pipedrive_field] = form_data[jotform_field]
        
        if not fields_to_update:
            print("No fields to update in Pipedrive")
            return
        
        # עדכון השדות בפייפדרייב
        api_url = f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PIPEDRIVE_API_KEY}"
        
        # ניסיון עם ריטריי לעדכן את השדות
        for attempt in range(3):
            try:
                response = requests.put(api_url, json=fields_to_update, timeout=10)
                
                if response.status_code == 200:
                    response_data = response.json()
                    if response_data.get("success"):
                        print(f"Successfully updated {len(fields_to_update)} fields for person {person_id}")
                        return True
                
                print(f"Failed to update fields in Pipedrive. Status code: {response.status_code}. Response: {response.text}")
                
            except requests.exceptions.RequestException as e:
                print(f"Request error when updating Pipedrive fields: {e}")
            
            # המתנה לפני ניסיון נוסף
            if attempt < 2:  # לא נמתין אחרי הניסיון האחרון
                await asyncio.sleep(1)
        
        return False
        
    except Exception as e:
        print(f"Error updating Pipedrive fields: {e}")
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
        
        # עדכון שדות פייפדרייב
        # נקרא לפונקציה החדשה לעדכון שדות
        await update_pipedrive_fields(person_id, form_data)
        
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
