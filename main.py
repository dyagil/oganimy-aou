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
    """נקודת קצה לקבלת התראות מ-JotForm ואחזור התשובות דרך API"""
    try:
        # קבלת מידע מ-JotForm כ-form data
        form_data = await request.form()
        print("================ JOTFORM WEBHOOK REQUEST =================")
        print(f"JotForm data received with keys: {', '.join(form_data.keys())}")
        
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

async def update_pipedrive_person(person_id, form_data):
    """יצירת פתק עם תשובות השאלון והצמדתו לכרטיס הלקוח בפייפדרייב"""
    try:
        # יצירת תוכן הפתק מכל התשובות בשאלון
        current_date = datetime.now().strftime("%d/%m/%Y %H:%M")
        note_content = f"**תשובות משאלון JotForm - {current_date}**\n\n"
        
        # קבלת המידע על כותרות השדות
        field_labels = {}
        if "_metadata" in form_data and "field_labels" in form_data["_metadata"]:
            field_labels = form_data["_metadata"]["field_labels"]
        
        # הוספת כל התשובות לפתק
        for field_name, field_value in form_data.items():
            # התעלם משדות מערכת וממזהה הלקוח
            if field_name != "typeA9" and not field_name.startswith("_"):
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
                
                note_content += f"**{field_label}**: {field_value}\n"
        
        print(f"Prepared note content from form data with {len(form_data)} fields")
        
        # יצירת הפתק בפייפדרייב
        note_payload = {
            "content": note_content,
            "person_id": person_id,
            "pinned_to_person_flag": 1  # נעיצת הפתק לכרטיס הלקוח
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
