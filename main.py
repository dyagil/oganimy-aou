from fastapi import FastAPI, Request
import requests
import os
import uvicorn
from dotenv import load_dotenv

# טען משתני סביבה מקובץ .env (אם קיים)
load_dotenv()

app = FastAPI()

JOTFORM_URL = "https://form.jotform.com/202432710986455"
PIPEDRIVE_API_KEY = os.getenv("PIPEDRIVE_API_KEY", "TO_BE_REPLACED")  # יוחלף ב-Railway כ-ENV

@app.post("/webhook")
async def handle_webhook(request: Request):
    # קבל את המידע מהבקשה
    data = await request.json()
    
    # הדפס את כל הבקשה לדיבוג
    print("================ WEBHOOK REQUEST =================")
    print(f"Full webhook data: {data}")
    
    # נסה לזהות את המבנה של הנתונים
    print("\nData Structure: " + ", ".join(data.keys()))
    
    # בדיקת קיום נתונים בסיסיים
    if not data:
        return {"status": "error", "message": "No data received"}
    
    # אם יש מבנה של אירוע - הדפס את המידע
    if "event" in data:
        print(f"Event Type: {data['event']}")
    
    # נסה לקבל את מזהה איש הקשר
    person_id = None
    paths_to_try = []
    
    # אם יש שדה data.id - זה המזהה שנצטרך להשתמש בו עבור ה-API
    if "data" in data and "id" in data["data"]:
        person_id = data["data"]["id"]
        print(f"Found person_id in data.id: {person_id}")
    # אם אין את זה, נסה את המיקומים האחרים
    else:
        # מסלולים אפשריים למזהה איש הקשר
        paths_to_try = [
            # מבנה מקורי
            lambda d: d.get("current", {}).get("person_id", {}).get("value"),
            # ישירות
            lambda d: d.get("id"),
            # עבור v2 API
            lambda d: d.get("current", {}).get("id"),
            # אם זה רשומת אנשי קשר
            lambda d: d.get("current", {}).get("id") if d.get("event") == "updated.person" else None,
            # מתוך previous
            lambda d: d.get("previous", {}).get("id"),
            # ממטא-דאטה - מידע שלא יעבוד עם API של Pipedrive
            lambda d: d.get("meta", {}).get("entity_id")
        ]
    
    # בדוק את כל האפשרויות למזהה רק אם עדיין לא מצאנו את המזהה
    if not person_id and paths_to_try:
        for i, path_func in enumerate(paths_to_try):
            try:
                val = path_func(data)
                if val:
                    person_id = val
                    print(f"Found person_id via path {i+1}: {person_id}")
                    break
            except Exception as e:
                print(f"Error checking path {i+1}: {e}")
    
    # בדיקת השדה המיוחד
    field_value = None
    
    # נסה למצוא את השדה המיוחד
    target_field_id = "51b05f4fe90c769c81299ac0d2bad3e75a02903e"  # המזהה שמחפשים
    field_value = None
    
    # בדוק ספציפית את המבנה שזיהינו בלוגים
    # 1. בדוק אם השדה נמצא בתוך custom_fields בתוך data
    if "data" in data and "custom_fields" in data["data"] and target_field_id in data["data"]["custom_fields"]:
        custom_field = data["data"]["custom_fields"][target_field_id]
        if custom_field and isinstance(custom_field, dict) and "id" in custom_field:
            field_value = custom_field["id"]
            print(f"Found field in data.custom_fields: {field_value}")
    
    # 2. בדוק אם השדה נמצא ישירות בתוך current
    elif "current" in data and "custom_fields" in data["current"] and target_field_id in data["current"]["custom_fields"]:
        custom_field = data["current"]["custom_fields"][target_field_id]
        if custom_field and isinstance(custom_field, dict) and "id" in custom_field:
            field_value = custom_field["id"]
            print(f"Found field in current.custom_fields: {field_value}")
    
    # 3. בדוק אם השדה נמצא ישירות בשורש
    elif "custom_fields" in data and target_field_id in data["custom_fields"]:
        custom_field = data["custom_fields"][target_field_id]
        if custom_field and isinstance(custom_field, dict) and "id" in custom_field:
            field_value = custom_field["id"]
            print(f"Found field in root.custom_fields: {field_value}")
    
    # הדפס מידע על כל שדה שמצאנו
    print("\nInspecting all available fields:")
    
    # עבור על כל המיקומים האפשריים
    locations_to_check = [
        ("data.custom_fields", data.get("data", {}).get("custom_fields", {})),
        ("current.custom_fields", data.get("current", {}).get("custom_fields", {})),
        ("root.custom_fields", data.get("custom_fields", {}))
    ]
    
    for location_name, location_data in locations_to_check:
        if isinstance(location_data, dict):
            print(f"Fields in {location_name}:")
            for key, value in location_data.items():
                print(f"  {key} = {value}")
                
                # בדוק אם זה השדה המיוחד
                if key == target_field_id and not field_value:
                    print(f"  >>> Found target field but couldn't extract id previously")
                    if isinstance(value, dict) and "id" in value:
                        field_value = value["id"]
                        print(f"  >>> Now extracted id: {field_value}")
    
    # חזור הודעת דיבוג אם חסרים נתונים
    if not field_value or not person_id:
        result = {"status": "ignored", "reason": f"Missing data - field: {bool(field_value)}, person: {bool(person_id)}"}
        print(f"Returning: {result}")
        return result
    
    print(f"\nProceeding with person_id: {person_id} and field_value: {field_value}")

    # שליפת פרטי איש הקשר מ-Pipedrive
    api_url = f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PIPEDRIVE_API_KEY}"
    print(f"Fetching person details from: {api_url}")
    
    try:
        person_res = requests.get(api_url)
        print(f"API Response Status: {person_res.status_code}")
        print(f"API Response: {person_res.text[:200]}...") # הדפס רק חלק מהתשובה למנוע לוגים ארוכים מדי
        
        person_data = person_res.json()
        if "data" not in person_data or not person_data["data"]:
            print(f"ERROR: Could not retrieve person data. Response: {person_res.text}")
            return {"status": "error", "message": "Could not retrieve person data"}
            
        person = person_data.get("data", {})
        print(f"Person data retrieved: {str(person)[:200]}...") # הדפס רק חלק מהנתונים
        
        # חילוץ פרטי איש הקשר
        first_name = person.get("first_name", "")
        last_name = person.get("last_name", "")
        
        # חילוץ מספר טלפון בדרך בטוחה יותר
        phone = ""
        if person.get("phone") and len(person.get("phone", [])) > 0:
            phone = person.get("phone", [{}])[0].get("value", "")
        
        # חילוץ אימייל בדרך בטוחה יותר
        email = ""
        if person.get("email") and len(person.get("email", [])) > 0:
            email = person.get("email", [{}])[0].get("value", "")
        
        print(f"Extracted details - Name: {first_name} {last_name}, Phone: {phone}, Email: {email}")
        
        # יצירת קישור לטופס JotForm
        jotform_link = f"{JOTFORM_URL}?first_name={first_name}&last_name={last_name}&phone={phone}&email={email}"
        print(f"Generated JotForm link: {jotform_link}")
        
        # הכנת הנתונים למשימה חדשה
        task_payload = {
            "subject": "מילוי טופס JotForm",
            "done": 0,
            "person_id": person_id,
            "note": f"היי! הנה הקישור שלך לטופס:\n{jotform_link}",
        }
        print(f"Task payload: {task_payload}")
        
        # יצירת משימה חדשה ב-Pipedrive
        task_url = f"https://api.pipedrive.com/v1/activities?api_token={PIPEDRIVE_API_KEY}"
        print(f"Creating task at: {task_url}")
        
        task_res = requests.post(task_url, json=task_payload)
        print(f"Task creation response status: {task_res.status_code}")
        print(f"Task creation response: {task_res.text[:200]}...") # הדפס רק חלק מהתשובה
        
        if task_res.status_code != 201 and task_res.status_code != 200:
            print(f"WARNING: Task creation might have failed. Status: {task_res.status_code}")
        
        return {"status": "created", "link": jotform_link}
        
    except Exception as e:
        print(f"ERROR in webhook processing: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return {"status": "error", "message": str(e)}

@app.get("/")
async def root():
    return {"message": "Pipedrive-JotForm Bridge is running. Use /webhook endpoint for integration."}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
