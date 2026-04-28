import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point

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

def fix_data_types(df):
    """
    Fix all column types, rename columns and set categorical orders.
    
    Args:
        df: DataFrame
    
    Returns:
        df: DataFrame with fixed column types
    """
    # Date
    df['date'] = pd.to_datetime(df['date'])
    
    # Departure and arrival times
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
    
    # Rename hour_rounded to full_hour
    df = df.rename(columns={'hour_rounded': 'full_hour'})
    
    # line_num to int
    df['line_num'] = pd.to_numeric(df['line_num'], errors='coerce').astype('Int64')
    
    # day - categorical with order
    day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    df['day'] = pd.Categorical(df['day'], categories=day_order, ordered=True)
    
    # string columns
    str_cols = ['line_name', 'alternative', 'agency_name', 'origin_city', 
                'origin_station', 'destination_city', 'destination_station', 'route_type']
    df[str_cols] = df[str_cols].astype(str)
    df['route_type'] = df['route_type'].str.strip()
    
    return df


def handle_missing_values(df, cols=['Total_Passengers']):
    """
    Handle missing values using progressive groupby strategy and mean imputation.
    
    Args:
        df: DataFrame
        cols: list of columns to fill with groupby median strategy
    
    Returns:
        df: DataFrame with filled missing values
    """
    # שלב 1-4 - groupby progressive strategy
    for col in cols:
        df[col] = df.groupby(['route_id', 'direction', 'day', 'full_hour'])[col].transform(
            lambda x: x.fillna(x.median())
        )
        print(f"{col} missing after step 1: {df[col].isna().sum()}")
        
        df[col] = df.groupby(['route_id', 'direction', 'day'])[col].transform(
            lambda x: x.fillna(x.median())
        )
        print(f"{col} missing after step 2: {df[col].isna().sum()}")
        
        df[col] = df.groupby(['route_id'])[col].transform(
            lambda x: x.fillna(x.median())
        )
        print(f"{col} missing after step 3: {df[col].isna().sum()}")
        
        df[col] = df[col].fillna(df[col].median())
        print(f"{col} missing after step 4: {df[col].isna().sum()}")
    
    # Impute duration and speed by agency, line and hour
    for col in ['duration_min_actual', 'duration_difference_min', 'speed_kmh_actual']:
        df = impute_by_agency_line_hour(df, col)
    
    # Impute geometric columns with mean
    geo_cols = ['curvity', 'perc_within_pt_route', 'route_length', 'length_in_buffer_m']
    for col in geo_cols:
        df[col] = df[col].fillna(df[col].mean())
    
    # Missing summary
    missing_summary = pd.DataFrame({
        'missing_count': df.isnull().sum(),
        'missing_percent': df.isnull().mean() * 100
    }).sort_values(by='missing_percent', ascending=False)
    
    print("\nMissing Summary:")
    print(missing_summary[missing_summary['missing_count'] > 0])
    
    return df


def fix_speed_and_duration(df):
    """
    Fix speed and duration columns:
    - Fix speed_kmh_planned > 100
    - Fix trips ending after midnight
    - Remove outliers in duration_difference_min
    
    Args:
        df: DataFrame
    
    Returns:
        df: DataFrame with fixed speed and duration columns
    """
    # Fix speed > 100
    mask_high = df['speed_kmh_planned'] > 100
    print(f"Rows with speed > 100: {mask_high.sum()}")
    df.loc[mask_high, 'speed_kmh_planned'] = df.loc[mask_high, 'speed_kmh_planned'] / 1000
    
    # Convert to datetime
    df['departure_time_planned'] = pd.to_datetime(df['departure_time_planned'].astype(str), format='mixed')
    df['arrival_time_planned'] = pd.to_datetime(df['arrival_time_planned'].astype(str), format='mixed')
    
    # Fix trips ending after midnight
    mask_midnight = df['arrival_time_planned'] < df['departure_time_planned']
    print(f"Trips ending after midnight: {mask_midnight.sum()}")
    
    df.loc[mask_midnight, 'duration_min_planned'] = (
        (df.loc[mask_midnight, 'arrival_time_planned'] + pd.Timedelta(days=1)) - 
        df.loc[mask_midnight, 'departure_time_planned']
    ).dt.total_seconds() / 60
    
    df.loc[mask_midnight, 'speed_kmh_planned'] = (
        (df.loc[mask_midnight, 'route_length'] / 1000) / 
        (df.loc[mask_midnight, 'duration_min_planned'] / 60)
    )
    
    # Remove outliers in duration_difference_min
    before = len(df)
    df = df[(df['duration_difference_min'] >= -120) & 
            (df['duration_difference_min'] <= 120)]
    after = len(df)
    print(f"Rows removed: {before - after:,} ({(before-after)/before*100:.2f}%)")
    print(f"Rows remaining: {after:,}")
    
    return df

def drop_unnecessary_columns(df):
    """
    Drop unnecessary columns from DataFrame.
    
    Args:
        df: DataFrame
    
    Returns:
        df: DataFrame without unnecessary columns
    """
    cols_to_drop = [
        'route_dir_alt_day_hr',
        'line_num_agency_alter_dir',
        'SIRI_id',
        'gtfs_ride_id',
        'gtfs_route_id'
    ]
    
    df = df.drop(columns=cols_to_drop, errors='ignore')
    print(f"Remaining columns: {df.shape[1]}")
    
    return df


def encode_categorical_columns(df):
    """
    Encode categorical columns using Ordinal, One-Hot and Target encoding.
    
    Args:
        df: DataFrame
    
    Returns:
        df: DataFrame with encoded categorical columns
    """
    # Ordinal encoding - day
    day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    day_mapping = {day: i for i, day in enumerate(day_order)}
    df['day_encoded'] = df['day'].map(day_mapping)
    
    # One-Hot encoding - alternative
    alternative_dummies = pd.get_dummies(df['alternative'], prefix='alternative')
    df = pd.concat([df, alternative_dummies], axis=1)
    
    # Target encoding
    from category_encoders import TargetEncoder
    target_cols = ['agency_name', 'origin_city', 'destination_city', 
                   'origin_station', 'destination_station']
    te = TargetEncoder()
    df[[f'{col}_encoded' for col in target_cols]] = te.fit_transform(
        df[target_cols], 
        df['duration_difference_min']
    )
    
    print(f"New columns added: day_encoded, {list(alternative_dummies.columns)}")
    print(f"Target encoded: {[f'{col}_encoded' for col in target_cols]}")
    
    return df    



from shapely.geometry import Point

def add_circular_route_flag(
    df,
    routes_linestring_path,
    route_id_col="route_id",
    linestring_col="linestring",
    threshold=500,
    crs="EPSG:2039"
):
    
    def is_circular(geom):
        if geom is None or geom.is_empty:
            return 0
        
        if geom.geom_type == "MultiLineString":
            if len(geom.geoms) == 0:
                return 0
            geom = geom.geoms[0]
        
        if len(geom.coords) < 2:
            return 0
        
        start = Point(geom.coords[0])
        end = Point(geom.coords[-1])
        
        dist = start.distance(end)
        
        return 1 if dist < threshold else 0

    # read route geometries
    df_linestring = pd.read_csv(routes_linestring_path)

    # convert WKT string to geometry
    df_linestring[linestring_col] = df_linestring[linestring_col].apply(
        lambda x: wkt.loads(x) if isinstance(x, str) and x.strip() != "" else None
    )

    # convert to GeoDataFrame
    gdf_linestring = gpd.GeoDataFrame(
        df_linestring,
        geometry=linestring_col,
        crs=crs
    )

    # calculate circular flag per route
    gdf_linestring["circular_route_flag"] = gdf_linestring[linestring_col].apply(is_circular)

    # avoid duplicated route_id values before merge
    route_flags = (
        gdf_linestring[[route_id_col, "circular_route_flag"]]
        .drop_duplicates(subset=route_id_col)
    )

    # merge back to original df
    df = df.merge(
        route_flags,
        on=route_id_col,
        how="left"
    )

    # routes missing from linestring file get 0
    df["circular_route_flag"] = df["circular_route_flag"].fillna(0).astype(int)

    return df
    