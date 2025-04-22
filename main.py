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
    
    # מסלולים אפשריים למזהה איש הקשר
    paths_to_try = [
        # מבנה מקורי
        lambda d: d.get("current", {}).get("person_id", {}).get("value"),
        # ממטא-דאטה
        lambda d: d.get("meta", {}).get("id"),
        # משדה data.data
        lambda d: d.get("data", {}).get("id"),
        # ישירות
        lambda d: d.get("id"),
        # עבור v2 API
        lambda d: d.get("current", {}).get("id"),
        # אם זה רשומת אנשי קשר
        lambda d: d.get("current", {}).get("id") if d.get("event") == "updated.person" else None,
        # מתוך previous
        lambda d: d.get("previous", {}).get("id")
    ]
    
    # בדוק את כל האפשרויות למזהה
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
    
    # נסה למצוא שדות בכל מקום אפשרי
    locations_to_check = [
        ("current", data.get("current", {})),
        ("data", data.get("data", {})),
        ("root", data)
    ]
    
    target_field_id = "51b05f4fe90c769c81299ac0d2bad3e75a02903e"  # המזהה שמחפשים
    
    # חיפוש בכל המיקומים האפשריים
    for location_name, location_data in locations_to_check:
        print(f"\nChecking fields in {location_name}:")
        if isinstance(location_data, dict):
            for key, value in location_data.items():
                # הדפס את כל השדות שמצאנו
                print(f"  Field: {key} = {value}")
                
                # בדוק התאמה למזהה השדה שאנחנו מחפשים
                if key == target_field_id:
                    field_value = value
                    print(f"  >>> Found target field {key} = {value}")
    
    # חזור הודעת דיבוג אם חסרים נתונים
    if not field_value or not person_id:
        result = {"status": "ignored", "reason": f"Missing data - field: {bool(field_value)}, person: {bool(person_id)}"}
        print(f"Returning: {result}")
        return result
    
    print(f"\nProceeding with person_id: {person_id} and field_value: {field_value}")

    person_res = requests.get(
        f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PIPEDRIVE_API_KEY}")
    person = person_res.json().get("data", {})

    first_name = person.get("first_name", "")
    last_name = person.get("last_name", "")
    phone = person.get("phone", [{}])[0].get("value", "")
    email = person.get("email", [{}])[0].get("value", "")

    jotform_link = f"{JOTFORM_URL}?first_name={first_name}&last_name={last_name}&phone={phone}&email={email}"

    task_payload = {
        "subject": "מילוי טופס JotForm",
        "done": 0,
        "person_id": person_id,
        "note": f"היי! הנה הקישור שלך לטופס:\n{jotform_link}",
    }

    requests.post(
        f"https://api.pipedrive.com/v1/activities?api_token={PIPEDRIVE_API_KEY}",
        json=task_payload
    )

    return {"status": "created", "link": jotform_link}

@app.get("/")
async def root():
    return {"message": "Pipedrive-JotForm Bridge is running. Use /webhook endpoint for integration."}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
