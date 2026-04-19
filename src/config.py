from datetime import timezone, timedelta
from pathlib import Path

BASE = "https://open-bus-stride-api.hasadna.org.il"

COLS_HEB_TO_ENG = {
    'תאריך':'date',
    'יום':'day',
    'שעה (עגולה)':'hour_rounded',
    'מספר קו':'line_num',
    'שם הקו':'line_name',
    'כיוון':'direction',
    'אלטרנטיבה':'alternative',
    'חברה (agency_name)':'agency_name',
    'סוג מסלול (route_type)':'route_type',
    'עיר מוצא':'origin_city',
    'תחנת מוצא':'origin_station',
    'עיר יעד':'destination_city',
    'תחנת יעד':'destination_station',
    'כמות תחנות':'number_of_stops',
    'אורך מסלול (קמ)':'route_length_km',
    'זמן יציאה מתוכנן':'departure_time_planned',
    'זמן הגעה מתוכנן':'arrival_time_planned',
    'משך מתוכנן (דק)':'duration_min_planned',
    'משך בפועל (דק)':'duration_min_actual',
    'הפרש (דק)':'duration_difference_min',
    'מהירות מתוכננת (קמש)':'speed_kmh_planned',
    'מהירות בפועל (קמש)':'speed_kmh_actual',
    'מזהה SIRI':'SIRI_id'
}

HEB_VALS_TO_ENG = {
    "ראשון": "Sunday",
    "שני": "Monday",
    "שלישי": "Tuesday",
    "רביעי": "Wednesday",
    "חמישי": "Thursday",
    "שישי": "Friday",
    "שבת": "Saturday",
    "דן": "Dan",
    "מטרופולין": "Metropolin",
    "קווים": "Kavim",
    "אגד": "Egged",
    "אלקטרה אפיקים": "Electra_Afikim",
    "נסיעות ותיירות": "Nesiot_ve_Tayarut",
    "תנופה": "Tnufa",
    "בית שמש אקספרס": "Bet_Shemesh_Express",
    "אודליה מוניות בעמ": "Odelya_Moniyot_LTD",
    "תבל": "Tevel",
    "רכבת ישראל": "Rakevet_Israel",
    "אוטובוס":"bus",
    "0": "light_train",
    "2":"Train",
    "8":"Service_taxi"
}

RAIN_COLS_HEB_TO_ENG = {
    "תחנה":"station",
    "תאריך ושעה (שעון קיץ)": "datetime",
    'כמות גשם (מ"מ)':  "rainfall_mm"
}

TZ = timezone(timedelta(hours=3))

WEEK_LABEL = "apr2024"
OUT_FILE   = Path(f"telaviv_buses_{WEEK_LABEL}.csv")

