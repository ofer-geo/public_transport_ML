import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
import math
from category_encoders import TargetEncoder

def fix_data_types(df):
    """Fix all column types and rename columns"""
    
    for col in df.select_dtypes(include=["datetime"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Hour
    if 'hour_rounded' in df.columns:
        df = df.rename(columns={'hour_rounded': 'full_hour'})
    
    
    # Day - categorical with order
    day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    df['day'] = pd.Categorical(df['day'], categories=day_order, ordered=True)
    
    # String columns
    str_cols = ['line_name', 'alternative', 'agency_name', 'origin_city', 
                'origin_station', 'destination_city', 'destination_station', 'route_type']
    df[str_cols] = df[str_cols].astype(str)
    df['route_type'] = df['route_type'].str.strip()

    
    
    return df


def handle_missing_values(df, ref_df=None, printing_missing_values=False):
    """
    Handle missing values.

    ref_df = Train df (for calculating medians/means)
    If ref_df=None, uses df itself (for train only)

    printing_missing_values:
        If True, prints missing values before and after the process.
    """

    if ref_df is None:
        ref_df = df

    if printing_missing_values:
        print("Missing values BEFORE handling:")
        print(df.isna().sum()[df.isna().sum() > 0].sort_values(ascending=False))
        print("-" * 50)

    # Convert day to string for groupby
    df['day'] = df['day'].astype(str)

    # Total_Passengers - progressive groupby imputation
    for col in ['Total_Passengers']:
        df[col] = df.groupby(['route_id', 'direction', 'day', 'full_hour'])[col].transform(
            lambda x: x.fillna(x.median())
        )

        df[col] = df.groupby(['route_id', 'direction', 'day'])[col].transform(
            lambda x: x.fillna(x.median())
        )

        df[col] = df.groupby(['route_id'])[col].transform(
            lambda x: x.fillna(x.median())
        )

        df[col] = df[col].fillna(ref_df[col].median())

    # Avg_Passengers_Per_Bus
    df['Avg_Passengers_Per_Bus'] = df.groupby('route_id')['Avg_Passengers_Per_Bus'].transform(
        lambda x: x.fillna(x.median())
    )
    df['Avg_Passengers_Per_Bus'] = df['Avg_Passengers_Per_Bus'].fillna(
        ref_df['Avg_Passengers_Per_Bus'].median()
    )

    # circular_route
    df['circular_route'] = df['circular_route'].fillna(0)

    # Text columns
    df["line_name"] = df["line_name"].fillna("unknown")
    df["agency_name"] = df["agency_name"].fillna("unknown")
    df["line_num"] = df["line_num"].fillna("unknown")

    # Speed and duration-related columns
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

    # Fix route_length zeros/NaNs using route_length_km
    if 'route_length' in df.columns and 'route_length_km' in df.columns:
        zero_mask = (df['route_length'] == 0) | (df['route_length'].isna())
        df.loc[zero_mask, 'route_length'] = df.loc[zero_mask, 'route_length_km'] * 1000

    if printing_missing_values:
        print("Missing values AFTER handling:")
        print(df.isna().sum()[df.isna().sum() > 0].sort_values(ascending=False))
        print("-" * 50)

    return df

def handle_outliers(df, boxplots=False, boxplot_cols=None, verbose=False):
    """
    Handle outliers:
    - Fix speed_kmh_planned > 100
    - Fix trips ending after midnight
    - Remove rows where duration_difference_min > 120 or < -120
    
    Parameters:
    df : DataFrame
    boxplots : bool
        If True, plots boxplots BEFORE and AFTER outlier handling.
    boxplot_cols : list
        Columns to include in boxplots.
    verbose : bool
        If True, prints process details.
    """

    # Default columns
    if boxplot_cols is None:
        boxplot_cols = [
            'full_hour',
            'route_length_km',
            'number_of_stops',
            'rainfall_mm',
            'Total_Passengers',
            'curvity',
            'duration_min_planned',
            'duration_min_actual',
            'speed_kmh_planned',
            'speed_kmh_actual',
            'duration_difference_min'
        ]

    # Keep only existing columns
    boxplot_cols = [col for col in boxplot_cols if col in df.columns]

    # 🔹 BEFORE plots
    if boxplots:
        print("\nBoxplots BEFORE outlier handling:")
        plot_boxplots_with_outliers(df, boxplot_cols)

    # --- Outlier handling ---

    # Fix speed > 100
    mask_high = df['speed_kmh_planned'] > 100
    if verbose:
        print(f"Rows with speed > 100: {mask_high.sum()}")
    df.loc[mask_high, 'speed_kmh_planned'] = df.loc[mask_high, 'speed_kmh_planned'] / 1000

    # Fix trips ending after midnight
    df['departure_time_planned'] = pd.to_datetime(df['departure_time_planned'].astype(str), format='mixed')
    df['arrival_time_planned'] = pd.to_datetime(df['arrival_time_planned'].astype(str), format='mixed')

    mask_midnight = df['arrival_time_planned'] < df['departure_time_planned']
    if verbose:
        print(f"Trips ending after midnight: {mask_midnight.sum()}")

    df.loc[mask_midnight, 'duration_min_planned'] = (
        (df.loc[mask_midnight, 'arrival_time_planned'] + pd.Timedelta(days=1)) -
        df.loc[mask_midnight, 'departure_time_planned']
    ).dt.total_seconds() / 60

    df.loc[mask_midnight, 'speed_kmh_planned'] = (
        (df.loc[mask_midnight, 'route_length'] / 1000) /
        (df.loc[mask_midnight, 'duration_min_planned'] / 60)
    )

    # Remove duration_difference_min outliers
    before = len(df)

    df = df[
        (df['duration_difference_min'] >= -120) &
        (df['duration_difference_min'] <= 120)
    ].copy()

    after = len(df)

    if verbose:
        print(f"Rows removed: {before - after:,} ({(before - after)/before*100:.2f}%)")
        print(f"Rows remaining: {after:,}")

    # 🔹 AFTER plots
    if boxplots:
        print("\nBoxplots AFTER outlier handling:")
        plot_boxplots_with_outliers(df, boxplot_cols)

    return df

def add_features(df):
    """Add new features"""
    df_copy = df.copy()
    peak_hours = [7, 8, 9, 14, 15, 16, 17]

    # Peak hour flag
    df_copy['is_peak_hour'] = df_copy['full_hour'].isin(peak_hours).astype(int)

    # Urban flag
    df_copy['urban'] = (df_copy['route_length'] <= 25000).astype(int)

    # Perc within PT route - safe division
    df_copy['perc_within_pt_route'] = np.where(
        df_copy['route_length'] > 0,
        df_copy['length_in_buffer_m'] / df_copy['route_length'],
        np.nan
    )

    # Remove possible inf values
    df_copy['perc_within_pt_route'] = df_copy['perc_within_pt_route'].replace(
        [np.inf, -np.inf], np.nan
    )

    # Optional: fill remaining NaN with 0
    df_copy['perc_within_pt_route'] = df_copy['perc_within_pt_route'].fillna(0)

    # Optional: clip unrealistic values
    df_copy['perc_within_pt_route'] = df_copy['perc_within_pt_route'].clip(0, 2)

    # Peak weighted perc - vectorized
    df_copy['perc_within_pt_route_peak'] = (
        df_copy['perc_within_pt_route'] * df_copy['is_peak_hour']
    )

    # Interaction features
    df_copy['passengers_x_peak'] = df_copy['Total_Passengers'] * df_copy['is_peak_hour']
    df_copy['stops_x_passengers'] = df_copy['number_of_stops'] * df_copy['Total_Passengers']

    # Arrival and departure hour features
    df_copy['departure_hour'] = pd.to_datetime(
        df_copy['departure_time_planned'], format='mixed'
    ).dt.hour

    df_copy['arrival_hour'] = pd.to_datetime(
        df_copy['arrival_time_planned'], format='mixed'
    ).dt.hour

    # Combined categorical feature
    df_copy['agency_linenum_dir_alter'] = (
        df_copy['agency_name'].astype(str) +
        df_copy['line_num'].astype(str) +
        df_copy['direction'].astype(str) +
        df_copy['alternative'].astype(str)
    )

    return df_copy


def encode_categorical_columns(df, te=None, alternative_cols=None):
    """
    Encode categorical columns.
    """
    # הסר כפילויות לפני הכל
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Ordinal encoding - day
    day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    day_mapping = {day: i for i, day in enumerate(day_order)}
    df['day_encoded'] = df['day'].map(day_mapping)
    
    
    # Target encoding
    from category_encoders import TargetEncoder
    target_cols = ['agency_name', 'origin_city', 'destination_city', 
                   'origin_station', 'destination_station','agency_linenum_dir_alter']
    
    if te is None:
        te = TargetEncoder()
        df[[f'{col}_encoded' for col in target_cols]] = te.fit_transform(
            df[target_cols], df['duration_difference_min']
        )
    else:
        df[[f'{col}_encoded' for col in target_cols]] = te.transform(
            df[target_cols]
        ) 
    
    # הסר כפילויות בסוף
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df, te, alternative_cols




def plot_boxplots_with_outliers(df, cols, n_cols=2, figsize=(20, 24)):
    """
    Plot boxplots with outlier statistics (IQR method)

    Parameters:
    df : DataFrame
    cols : list of column names
    n_cols : number of subplot columns
    figsize : figure size
    """
    
    n_rows = math.ceil(len(cols) / n_cols)
    
    plt.figure(figsize=figsize)
    
    for i, col in enumerate(cols):
        ax = plt.subplot(n_rows, n_cols, i + 1)
        
        sns.boxplot(data=df, x=col, ax=ax, color='navy')
        
        # IQR calculation
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        
        n_outliers = len(df[(df[col] < lower) | (df[col] > upper)])
        pct_outliers = (n_outliers / len(df)) * 100
        
        ax.set_title(
            f'{col}\nOutliers: {n_outliers:,} ({pct_outliers:.1f}%)',
            fontsize=11
        )
        ax.set_xlabel(col)
    
    plt.subplots_adjust(hspace=0.5, wspace=0.3)
    plt.tight_layout()
    plt.show()

def drop_unnecessary_columns(df):
    """Drop columns that are already encoded, not relevant, or cause data leakage"""
    
    cols_to_drop = [
        # כבר מקודדות
        'day', 'alternative', 'agency_name', 'origin_city',
        'origin_station', 'destination_city', 'destination_station',
        # זהות לעמודות אחרות
        'route_mkt', 'route_length_kn',
        # טקסט/זמן
        'date', 'line_name', 'departure_time_planned',
        'arrival_time_planned', 'route_type', 'line_num','agency_linenum_dir_alter',
        # Data Leakage
        'duration_min_actual', 'duration_min_planned', 'speed_kmh_actual',

    ]
    
    df = df.drop(columns=cols_to_drop, errors='ignore')
    
    return df

def run_feature_selection_methods(X_train, y_train):
    """
    Run multiple feature selection methods for a regression problem.

    Returns:
    selection_df : DataFrame with selected/not selected features per method
    """

    import numpy as np
    import pandas as pd

    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import Lasso, Ridge
    from sklearn.svm import LinearSVR
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    methods = [
        "RandomForest",
        "GradientBoost",
        "Lasso",
        "SVR",
        "Ridge"
    ]

    print("Feature selection methods to be used:")
    for i, method in enumerate(methods, start=1):
        print(f"{i}. {method}")

    print("-" * 50)

    total_methods = len(methods)
    finished = 0

    # 1. Random Forest
    rf = RandomForestRegressor(random_state=42).fit(X_train, y_train)
    rf_selected = (rf.feature_importances_ > 0).astype(int)

    finished += 1
    print(f"Finished RandomForest ({finished}/{total_methods})")

    # 2. Gradient Boosting
    gb = GradientBoostingRegressor(random_state=42).fit(X_train, y_train)
    gb_selected = (gb.feature_importances_ > 0).astype(int)

    finished += 1
    print(f"Finished GradientBoost ({finished}/{total_methods})")

    # 3. Lasso
    lasso = make_pipeline(
        StandardScaler(),
        Lasso(alpha=0.1, max_iter=10000, random_state=42)
    )

    lasso.fit(X_train, y_train)

    lasso_coef = lasso.named_steps["lasso"].coef_
    lasso_selected = (np.abs(lasso_coef) > 0).astype(int)

    finished += 1
    print(f"Finished Lasso ({finished}/{total_methods})")

    # 4. Linear SVR
    svr = make_pipeline(
        StandardScaler(),
        LinearSVR(C=0.01, max_iter=10000, random_state=42)
    )

    svr.fit(X_train, y_train)

    svr_coef = svr.named_steps["linearsvr"].coef_
    svr_selected = (np.abs(svr_coef) > 0).astype(int)

    finished += 1
    print(f"Finished SVR ({finished}/{total_methods})")

    # 5. Ridge
    ridge = make_pipeline(
        StandardScaler(),
        Ridge(alpha=0.01)
    )

    ridge.fit(X_train, y_train)

    ridge_coef = ridge.named_steps["ridge"].coef_
    ridge_selected = (np.abs(ridge_coef) > 0).astype(int)

    finished += 1
    print(f"Finished Ridge ({finished}/{total_methods})")

    # Create results DataFrame
    selection_df = pd.DataFrame({
        "Feature": X_train.columns,
        "Lasso": lasso_selected,
        "SVR": svr_selected,
        "GradientBoost": gb_selected,
        "RandomForest": rf_selected,
        "Ridge": ridge_selected
    })

    # Sum selections
    selection_cols = ["Lasso", "SVR", "GradientBoost", "RandomForest", "Ridge"]

    selection_df["Sum"] = selection_df[selection_cols].sum(axis=1)

    selection_df = selection_df.sort_values(
        by="Sum",
        ascending=False
    ).reset_index(drop=True)

    return selection_df


def manipulate_df_process(df):
    df = fix_data_types(df)
    df = handle_missing_values(df, ref_df=None)
    df = handle_outliers(df)
    df = add_features(df)
    df, te, alternative_cols = encode_categorical_columns(df, te=None, alternative_cols=None)
    
    return df


def compare_xgb_feature_sets(X_train, y_train, X_val, y_val, selection_df):
    """
    Compare XGBoost performance on:
    1. All features
    2. Features selected by all models (Sum == 5)
    3. Features selected by >=4 models

    Returns:
    results_df
    """

    import numpy as np
    import pandas as pd
    from xgboost import XGBRegressor
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

    results = []

    # Define feature sets
    feature_sets = {
        "All Features": X_train.columns.tolist(),
        "Sum == 5": selection_df.loc[selection_df["Sum"] == 5, "Feature"].tolist(),
        "Sum >= 4": selection_df.loc[selection_df["Sum"] >= 4, "Feature"].tolist()
    }

    print("Running XGBoost on feature sets:")
    for name, feats in feature_sets.items():
        print(f"- {name}: {len(feats)} features")

    print("-" * 50)

    # Loop over feature sets
    for name, feats in feature_sets.items():

        if len(feats) == 0:
            print(f"Skipping {name} (no features)")
            continue

        # Subset data
        X_tr = X_train[feats]
        X_vl = X_val[feats]

        # Model
        model = XGBRegressor(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            random_state=42
        )

        model.fit(X_tr, y_train)

        y_pred = model.predict(X_vl)

        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        mae = mean_absolute_error(y_val, y_pred)
        r2 = r2_score(y_val, y_pred)

        print(f"{name} → RMSE: {rmse:.3f}, MAE: {mae:.3f}, R²: {r2:.3f}")

        results.append({
            "Feature_Set": name,
            "Num_Features": len(feats),
            "RMSE": rmse,
            "MAE": mae,
            "R2": r2
        })

    results_df = pd.DataFrame(results).sort_values(by="RMSE")

    return results_df