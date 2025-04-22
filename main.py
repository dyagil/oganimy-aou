from fastapi import FastAPI, Request
import requests

app = FastAPI()

JOTFORM_URL = "https://form.jotform.com/202432710986455"
PIPEDRIVE_API_KEY = "TO_BE_REPLACED"  # יוחלף ב-Railway כ-ENV

@app.post("/webhook")
async def handle_webhook(request: Request):
    data = await request.json()
    person_id = data.get("current", {}).get("person_id", {}).get("value")
    field_value = data.get("current", {}).get("51b05f4fe90c769c81299ac0d2bad3e75a02903e")

    if not field_value:
        return {"status": "ignored"}

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
