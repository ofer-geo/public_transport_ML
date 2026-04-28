import pandas as pd

def rename_columns(df,cols_names_dict):
    return df.rename(columns=cols_names_dict)

def rename_weekdays(df, values_heb_to_eng_dict):
    df["day"] = df["day"].map(values_heb_to_eng_dict)

def convert_data_types(df):
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y").dt.date
    df["hour_rounded"] = df["hour_rounded"].str[:2].astype(int)
    df["departure_time_planned"] = pd.to_datetime(df["departure_time_planned"], format="%H:%M:%S").dt.time
    df["arrival_time_planned"] = pd.to_datetime(df["arrival_time_planned"], format="%H:%M:%S").dt.time
    df['SIRI_id'] = df['SIRI_id'].astype(int)


def translate_columns(df, columns, mapping):
    df = df.copy()

    for col in columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .map(mapping)
        )

    return df


from src.time_utils import fmt_date, fmt_time, day_he, round_hour, dur_min

def build_rows(rides, route_cache):
    rows = []

    for ride in rides:
        dep = ride.get("scheduled_start_time", "")
        arr = ride.get("gtfs_ride__end_time", "")

        plan = dur_min(dep, arr)
        act = ride.get("duration_minutes") if ride.get("duration_minutes", 0) > 0 else ""
        diff = (act - plan) if isinstance(plan, int) and isinstance(act, int) else ""

        s = route_cache.get(ride.get("gtfs_ride__gtfs_route_id"), {})
        dist = s.get("dist_km", 0)

        rows.append({
            "תאריך": fmt_date(dep),
            "יום": day_he(dep),
            "שעה (עגולה)": round_hour(dep),

            "מספר קו": ride.get("gtfs_route__route_short_name", ""),
            "שם הקו": ride.get("gtfs_route__route_long_name", ""),
            "route_id": ride.get("gtfs_route__line_ref", ""),
            "route_mkt": ride.get("gtfs_route__route_mkt", ""),

            "כיוון": ride.get("gtfs_route__route_direction", ""),
            "אלטרנטיבה": ride.get("gtfs_route__route_alternative", ""),
            "חברה (agency_name)": ride.get("gtfs_route__agency_name", ""),

            "סוג מסלול (route_type)":
                "אוטובוס" if ride.get("gtfs_route__route_type") == "3"
                else ride.get("gtfs_route__route_type", ""),

            "עיר מוצא": s.get("from_city", ""),
            "תחנת מוצא": s.get("from_stop", ""),
            "עיר יעד": s.get("to_city", ""),
            "תחנת יעד": s.get("to_stop", ""),

            "כמות תחנות": s.get("stop_count", ""),
            "אורך מסלול (קמ)": dist or "",

            "זמן יציאה מתוכנן": fmt_time(dep),
            "זמן הגעה מתוכנן": fmt_time(arr),

            "משך מתוכנן (דק)": plan,
            "משך בפועל (דק)": act,
            "הפרש (דק)": diff,

            "מהירות מתוכננת (קמש)":
                round(dist / (plan / 60), 1)
                if dist and isinstance(plan, int) and plan > 0 else "",

            "מהירות בפועל (קמש)":
                round(dist / (act / 60), 1)
                if dist and isinstance(act, int) and act > 0 else "",

            "gtfs_route_id": ride.get("gtfs_ride__gtfs_route_id", ""),
            "gtfs_ride_id": ride.get("gtfs_ride_id", ""),
            "מזהה SIRI": ride.get("id", ""),
        })

    df = pd.DataFrame(rows)

    return df

def fix_datetime_columns(df):
    """
    Fix departure and arrival time columns by combining date with time.
    
    Args:
        df: DataFrame with columns: date, departure_time_planned, arrival_time_planned
    
    Returns:
        df: DataFrame with fixed datetime columns
    """
    df['date'] = pd.to_datetime(df['date'])
    
    df['departure_time_planned'] = pd.to_datetime(
        df['date'].dt.strftime('%Y-%m-%d') + ' ' + 
        pd.to_datetime(df['departure_time_planned'], format='mixed').dt.strftime('%H:%M:%S'),
        format='%Y-%m-%d %H:%M:%S'
    )
    
    df['arrival_time_planned'] = pd.to_datetime(
        df['date'].dt.strftime('%Y-%m-%d') + ' ' + 
        pd.to_datetime(df['arrival_time_planned'], format='mixed').dt.strftime('%H:%M:%S'),
        format='%Y-%m-%d %H:%M:%S'
    )
    
    return df    

def fix_column_types(df):
    """
    Fix column types and rename columns.
    """
    df = df.rename(columns={'hour_rounded': 'full_hour'})
    df['line_num'] = pd.to_numeric(df['line_num'], errors='coerce').astype('Int64')
    return df


def fix_categorical_columns(df):
    """
    Fix categorical and string columns.
    """
    day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    df['day'] = pd.Categorical(df['day'], categories=day_order, ordered=True)
    
    str_cols = ['line_name', 'alternative', 'agency_name', 'origin_city', 
                'origin_station', 'destination_city', 'destination_station', 'route_type']
    df[str_cols] = df[str_cols].astype(str)
    df['route_type'] = df['route_type'].str.strip()
    
    return df
    