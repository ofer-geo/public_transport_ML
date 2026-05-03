import pandas as pd

def impute_by_agency_line_hour(df, target_col, agg='mean'):
    """
    Impute missing values in a column using hierarchical grouping:
    1. agency_name + line_num + full_hour
    2. agency_name + line_num
    3. agency_name
    4. global aggregation

    Parameters:
    - df: DataFrame
    - target_col: column to impute
    - agg: aggregation method ('mean', 'median', etc.)
    """

    initial_missing = df[target_col].isna().sum()

    # level 1
    df[target_col] = df[target_col].fillna(
        df.groupby(['agency_name', 'line_num', 'full_hour'])[target_col].transform(agg)
    )

    # level 2
    df[target_col] = df[target_col].fillna(
        df.groupby(['agency_name', 'line_num'])[target_col].transform(agg)
    )

    # level 3
    df[target_col] = df[target_col].fillna(
        df.groupby(['agency_name'])[target_col].transform(agg)
    )

    # global fallback
    df[target_col] = df[target_col].fillna(df[target_col].agg(agg))

    filled = initial_missing - df[target_col].isna().sum()
    print(f"{target_col}: filled {filled} values using '{agg}'")

    return df


def add_features(df):
    """Add new features"""
    
    peak_hours = [7, 8, 9, 14, 15, 16, 17]
    
    # Peak hour flag
    df['is_peak_hour'] = df['full_hour'].isin(peak_hours).astype(int)
    
    # Urban flag
    df['urban'] = df['route_length'] <= 25000
    
    # Perc within pt route
    df['perc_within_pt_route'] = df['length_in_buffer_m'] / df['route_length']
    
    # Peak weighted perc
    df['perc_within_pt_route_peak'] = df.apply(
        lambda row: row['perc_within_pt_route'] if row['full_hour'] in peak_hours else 0, axis=1
    )
    
    # Interaction features
    df['passengers_x_peak'] = df['Total_Passengers'] * df['is_peak_hour']
    df['stops_x_passengers'] = df['number_of_stops'] * df['Total_Passengers']
    
    # arrival_hour and departure_hour features
    df['departure_hour'] = pd.to_datetime(df['departure_time_planned'], format='mixed').dt.hour
    df['arrival_hour'] = pd.to_datetime(df['arrival_time_planned'], format='mixed').dt.hour


    
    return df


def fix_data_types(df):
    """Fix all column types and rename columns"""
    
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
    
    # Hour
    if 'hour_rounded' in df.columns:
        df = df.rename(columns={'hour_rounded': 'full_hour'})
    
    # Line_num
    df['line_num'] = pd.to_numeric(df['line_num'], errors='coerce').astype('Int64')
    
    # Day - categorical with order
    day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    df['day'] = pd.Categorical(df['day'], categories=day_order, ordered=True)
    
    # String columns
    str_cols = ['line_name', 'alternative', 'agency_name', 'origin_city', 
                'origin_station', 'destination_city', 'destination_station', 'route_type']
    df[str_cols] = df[str_cols].astype(str)
    df['route_type'] = df['route_type'].str.strip()

    # Bool columns
    df['urban'] = df['urban'].astype(int)
    
    return df


def handle_missing_values(df, ref_df=None):
    """
    Handle missing values.
    ref_df = Train df (for calculating medians/means)
    If ref_df=None, uses df itself (for train only)
    """
    if ref_df is None:
        ref_df = df
    
    # המר day ל-string לצורך groupby
    df['day'] = df['day'].astype(str)
    
    # Step 1-4: Progressive groupby for Total_Passengers
    for col in ['Total_Passengers']:
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
        
        df[col] = df[col].fillna(ref_df[col].median())
        print(f"{col} missing after step 4: {df[col].isna().sum()}")
    
    # מלא פיצ'רים שתלויים ב-Total_Passengers
    df['passengers_x_peak'] = df['Total_Passengers'] * df['is_peak_hour']
    df['stops_x_passengers'] = df['number_of_stops'] * df['Total_Passengers']
    
    # Avg_Passengers_Per_Bus
    df['Avg_Passengers_Per_Bus'] = df.groupby('route_id')['Avg_Passengers_Per_Bus'].transform(
        lambda x: x.fillna(x.median())
    )
    df['Avg_Passengers_Per_Bus'] = df['Avg_Passengers_Per_Bus'].fillna(ref_df['Avg_Passengers_Per_Bus'].median())
    
    # perc_within_pt_route
    df['perc_within_pt_route'] = df['perc_within_pt_route'].fillna(ref_df['perc_within_pt_route'].mean())
    
    # חשב מחדש perc_within_pt_route_peak
    peak_hours = [7, 8, 9, 14, 15, 16, 17]
    df['perc_within_pt_route_peak'] = df.apply(
        lambda row: row['perc_within_pt_route'] if row['full_hour'] in peak_hours else 0, axis=1
    )
    
    # circular_route
    df['circular_route'] = df['circular_route'].fillna(0)
    
    # speed, duration
    for col in ['speed_kmh_actual', 'duration_min_actual', 'duration_difference_min']:
        df[col] = df.groupby(['route_id', 'direction'])[col].transform(
            lambda x: x.fillna(x.median())
        )
        df[col] = df[col].fillna(ref_df[col].median())
    
    # Geometric columns
    geo_cols = ['curvity', 'route_length', 'length_in_buffer_m']
    for col in geo_cols:
        if col in df.columns:
            df[col] = df[col].fillna(ref_df[col].mean())
    
    # Missing summary
    missing_summary = pd.DataFrame({
        'missing_count': df.isnull().sum(),
        'missing_percent': df.isnull().mean() * 100
    }).sort_values(by='missing_percent', ascending=False)
    
    print("\nMissing Summary:")
    print(missing_summary[missing_summary['missing_count'] > 0])
    
    return df