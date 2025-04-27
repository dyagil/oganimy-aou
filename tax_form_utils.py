"""
מודול עזר לטיפול בשאלוני מס מג'וטפורם והעלאתם לדילים בפייפדרייב
"""
import json
import traceback
import requests
from datetime import datetime
import re

# מילון לתרגום שמות שדות משאלון המס לשדות המתאימים בפייפדרייב
TAX_FORM_FIELD_MAPPING = {
    # נתונים אישיים
    "input109": "מספר תעודת זהות",
    "typeA": "מספר תעודת זהות",
    "input117": "תאריך לידה",
    "BB-DATHE": "תאריך לידה",
    "input107": "מצב משפחתי",
    "typeA21": "מצב משפחתי",
    "typeA23": "מספר ילדים",
    
    # נדל"ן ומשכנתאות
    "input89": "יש דירה בבעלות",
    "input90": "יש משכנתה פעילה",
    "input91": "מעוניין במשכנתא הפוכה",
    "input92": "מעוניין בדירה",
    
    # ייעוץ פיננסי וביטוח
    "input66": "מעוניין בייעוץ פיננסי פנסיוני",
    "input93": "מעוניין בייעוץ פרישה",
    "input94": "מעוניין בבדיקת ביטוחים",
    "input67": "מעוניין בבדיקה מול ביטוח לאומי",
    "input96": "מעוניין בבדיקה מול ביטוח לאומי",
    
    # שונות
    "input123": "נוסע לחו\"ל בחצי שנה הקרובה"
}

# מילון לתרגום קטגוריות בפתק בפייפדרייב
TAX_FORM_CATEGORIES = {
    "נתונים אישיים": ["input109", "typeA", "input117", "BB-DATHE", "input107", "typeA21", "typeA23"],
    "נדל\"ן ומשכנתאות": ["input89", "input90", "input91", "input92"],
    "ייעוץ פיננסי וביטוח": ["input66", "input93", "input94", "input67", "input96"],
    "שונות": ["input123"]
}

# רשימת שדות להתעלם מהם
TAX_FORM_IGNORED_FIELDS = [
    "typeA8", "typeA9", "date", "ip", "date124", "form_name", 
    "submission_id", "control_text", "control_text_2", "control_6", "control_1",
    "website", "submit", "submission_id", "view", "anchor1", "anchor2", "anchor3", "anchor4",
    "submit_form", "preferred_time", "signature", "form_title", "form_id"
]

# מילות מפתח לשדות שלא רוצים להציג
TAX_FORM_IGNORED_TEXTS = [
    "עוגנים", "anchor", "אנכור", 
    "תודה שמילאת", 
    "אישורי", "הערות", 
    "הוראות", "חתימה"
]

async def update_pipedrive_deal_with_tax_form(api_key, deal_id, form_data):
    """
    יצירת פתק עם תשובות שאלון מס והצמדתו לדיל בפייפדרייב
    """
    try:
        if not deal_id:
            print("Missing deal_id for tax form update")
            return False

        # יצירת תוכן הפתק מכל התשובות בשאלון
        current_date = datetime.now().strftime("%d/%m/%Y %H:%M")
        note_content = f"# תשובות שאלון מס {current_date}\n\n"
        
        # קבלת המידע על כותרות השדות
        field_labels = {}
        if "_metadata" in form_data and "field_labels" in form_data["_metadata"]:
            field_labels = form_data["_metadata"]["field_labels"]
        
        # ערכים שיחשבו כריקים
        empty_values = ["", "null", "undefined", None, "0", "-", "N/A", "n/a", "לא"]
        
        # מיון שדות לקטגוריות
        categories = {
            "נתונים אישיים": [],
            "נדל\"ן ומשכנתאות": [],
            "ייעוץ פיננסי וביטוח": [],
            "שונות": []
        }
        
        # זיהוי שדות שהשם שלהם זהה לערך (כמו שאלות כן/לא בג'וטפורם)
        def clean_value(value):
            """ ניקוי הערך מתווים מיוחדים להשוואה """
            if not value:
                return ""
            return str(value).lower().strip().replace("?", "").replace(":", "")
        
        # עיבוד השדות לקטגוריות
        items_added = 0
        
        for field_name, field_value in form_data.items():
            # התעלם משדות להתעלמות, שדות מערכת ושדות ריקים
            if (field_name not in TAX_FORM_IGNORED_FIELDS and 
                not field_name.startswith("_") and 
                field_value not in empty_values and 
                str(field_value).strip() != ""):
                
                # קבלת הכותרת האמיתית אם קיימת במיפוי
                if field_name in TAX_FORM_FIELD_MAPPING:
                    field_label = TAX_FORM_FIELD_MAPPING[field_name]
                elif field_name in field_labels and field_labels[field_name].strip():
                    field_label = field_labels[field_name]
                else:
                    field_label = field_name
                    if "_" in field_name:
                        field_label = field_name.split("_")[-1]
                    
                    # טיפול בשדות מיוחדים
                    if field_name.startswith("input"):
                        field_label = f"שאלה {field_name.replace('input', '')}"
                    elif field_name.startswith("typeA"):
                        field_label = f"שדה {field_name.replace('typeA', '')}"
                
                # בדיקה אם השדה או הערך מכיל טקסט שרוצים להתעלם ממנו
                should_ignore = False
                for ignored_text in TAX_FORM_IGNORED_TEXTS:
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
                        field_value = "✓"  # סימון של "כן"
                    elif clean_value_str == "לא" or clean_value_str == "no" or clean_value_str == "false":
                        field_value = "✗"  # סימון של "לא"
                    else:
                        field_value = "✓"  # נשתמש בסימון להראות שהלקוח בחר זאת
                
                if should_ignore:
                    continue
                
                # הכנת הערך המעוצב לתצוגה
                if field_value == "✓":
                    # אם זו שאלת כן/לא או ערך זהה לשם השדה - נציג עם סימון ✓
                    formatted_field = f"○ {field_label} ✓"
                elif field_value == "✗":
                    # אם זו שאלת כן/לא שסומנה כ'לא' - נציג עם סימון ✗
                    formatted_field = f"○ {field_label} ✗"
                else:
                    # אחרת - נציג את שם השדה והערך
                    formatted_field = f"○ {field_label}: {field_value}"
                
                # הוספה לקטגוריה המתאימה לפי מיפוי השדות
                category_assigned = False
                for category, field_list in TAX_FORM_CATEGORIES.items():
                    if field_name in field_list:
                        categories[category].append(formatted_field)
                        category_assigned = True
                        break
                
                # אם לא מצאנו קטגוריה מתאימה - נוסיף לשונות
                if not category_assigned:
                    categories["שונות"].append(formatted_field)
                
                items_added += 1
        
        # הסרת כפילויות בכל קטגוריה
        for category in categories:
            unique_fields = {}
            for field in categories[category]:
                field_parts = field.split("○ ")
                if len(field_parts) >= 2:
                    field_title = field_parts[1].split(" ✓")[0].split(" ✗")[0].split(":")[0].strip()
                    if field_title not in unique_fields or len(field) < len(unique_fields[field_title]):
                        unique_fields[field_title] = field
            
            # החלפת הרשימה המקורית עם הרשימה ללא כפילויות
            categories[category] = list(unique_fields.values())
            
            # מיון השדות לפי א-ב
            categories[category].sort()
        
        # בניית תוכן הפתק לפי קטגוריות
        for category, fields in categories.items():
            if fields:  # רק אם יש שדות בקטגוריה
                note_content += f"## {category}\n"
                for field in fields:
                    note_content += f"{field}\n"
                note_content += "\n"
        
        # הוספת פתק רק אם יש תוכן משמעותי
        if items_added > 0:
            # שליחת הנתונים לפייפדרייב
            note_data = {
                "content": note_content,
                "deal_id": deal_id
            }
            
            # שליחת הבקשה ליצירת פתק
            note_url = f"https://api.pipedrive.com/v1/notes?api_token={api_key}"
            response = requests.post(note_url, json=note_data)
            
            print(f"Added tax form note to deal {deal_id} with {items_added} items.")
            
            if response.status_code in [200, 201]:
                return True
            else:
                print(f"Failed to add note to deal. Status code: {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return False
        else:
            print(f"No valid form fields found, skipping note creation for deal {deal_id}")
            return False
    
    except Exception as e:
        print(f"ERROR creating note for tax form: {str(e)}")
        print(traceback.format_exc())
        return False

def is_tax_form_submission(form_id, form_title):
    """
    בדיקה אם מדובר בשאלון מס לפי מזהה הטופס או הכותרת שלו
    """
    # המזהה הידוע של שאלון המס
    tax_form_id = "222503111662038"
    
    # בדיקה לפי מזהה
    if form_id == tax_form_id:
        return True
    
    # בדיקה לפי כותרת
    tax_keywords = [
        "החזר מס", "שאלון מס", "שאלון להחזר מס", "tax return"
    ]
    
    for keyword in tax_keywords:
        if keyword.lower() in form_title.lower():
            return True
    
    return False
