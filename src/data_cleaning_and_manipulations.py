import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
import math
from category_encoders import TargetEncoder
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, accuracy_score, f1_score
from sklearn.preprocessing import LabelEncoder


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

def fill_by_ref_group_median(df, ref_df, group_cols, value_col):
    """
    Fill missing values using group medians calculated ONLY from ref_df.
    """

    medians = (
        ref_df.groupby(group_cols)[value_col]
        .median()
        .rename("_median")
        .reset_index()
    )

    df = df.merge(medians, on=group_cols, how="left")

    df[value_col] = df[value_col].fillna(df["_median"])

    df = df.drop(columns="_median")

    return df


def handle_missing_values(df, ref_df=None, printing_missing_values=False):
    """
    Handle missing values without data leakage.

    ref_df:
        Train dataframe used for calculating all statistics.
        If None -> uses df itself (for training data).

    printing_missing_values:
        If True, prints missing values before and after.
    """

    import pandas as pd
    import numpy as np

    df = df.copy()

    if ref_df is None:
        ref_df = df.copy()

    if printing_missing_values:
        print("Missing values BEFORE handling:")
        print(df.isna().sum()[df.isna().sum() > 0].sort_values(ascending=False))
        print("-" * 50)

    # Convert day to string
    df['day'] = df['day'].astype(str)
    ref_df['day'] = ref_df['day'].astype(str)

    # =========================================================
    # Total_Passengers
    # =========================================================

    for col in ['Total_Passengers']:

        df = fill_by_ref_group_median(
            df,
            ref_df,
            ['route_id', 'direction', 'day', 'full_hour'],
            col
        )

        df = fill_by_ref_group_median(
            df,
            ref_df,
            ['route_id', 'direction', 'day'],
            col
        )

        df = fill_by_ref_group_median(
            df,
            ref_df,
            ['route_id'],
            col
        )

        df[col] = df[col].fillna(ref_df[col].median())

    # =========================================================
    # Avg_Passengers_Per_Bus
    # =========================================================

    df = fill_by_ref_group_median(
        df,
        ref_df,
        ['route_id'],
        'Avg_Passengers_Per_Bus'
    )

    df['Avg_Passengers_Per_Bus'] = df[
        'Avg_Passengers_Per_Bus'
    ].fillna(
        ref_df['Avg_Passengers_Per_Bus'].median()
    )

    # =========================================================
    # Circular route
    # =========================================================

    df['circular_route'] = df['circular_route'].fillna(0)

    # =========================================================
    # Text columns
    # =========================================================

    df["line_name"] = df["line_name"].fillna("unknown")
    df["agency_name"] = df["agency_name"].fillna("unknown")
    df["line_num"] = df["line_num"].fillna("unknown")

    # =========================================================
    # Speed / duration columns
    # =========================================================

    for col in [
        'speed_kmh_actual',
        'duration_min_actual',
        'duration_difference_min'
    ]:

        df = fill_by_ref_group_median(
            df,
            ref_df,
            ['route_id', 'direction'],
            col
        )

        df[col] = df[col].fillna(ref_df[col].median())

    # =========================================================
    # Geometric columns
    # =========================================================

    geo_cols = [
        'curvity',
        'route_length',
        'length_in_buffer_m'
    ]

    for col in geo_cols:
        if col in df.columns:
            df[col] = df[col].fillna(ref_df[col].mean())

    # =========================================================
    # Fix route_length
    # =========================================================

    if 'route_length' in df.columns and 'route_length_km' in df.columns:

        zero_mask = (
            (df['route_length'] == 0) |
            (df['route_length'].isna())
        )

        df.loc[zero_mask, 'route_length'] = (
            df.loc[zero_mask, 'route_length_km'] * 1000
        )

    # =========================================================
    # Print remaining missing
    # =========================================================

    if printing_missing_values:
        print("Missing values AFTER handling:")
        print(df.isna().sum()[df.isna().sum() > 0].sort_values(ascending=False))
        print("-" * 50)

    return df


def handle_outliers(df, boxplots=False, boxplot_cols=None, verbose=False, remove_target_outliers=True):
    """
    Handle outliers:
    - Fix speed_kmh_planned > 100
    - Fix trips ending after midnight
    - Optionally remove duration_difference_min outliers
    """

    import pandas as pd

    df = df.copy()

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

    boxplot_cols = [col for col in boxplot_cols if col in df.columns]

    if boxplots:
        print("\nBoxplots BEFORE outlier handling:")
        plot_boxplots_with_outliers(df, boxplot_cols)

    # Fix speed > 100
    if 'speed_kmh_planned' in df.columns:
        mask_high = df['speed_kmh_planned'] > 100

        if verbose:
            print(f"Rows with speed > 100: {mask_high.sum()}")

        df.loc[mask_high, 'speed_kmh_planned'] = (
            df.loc[mask_high, 'speed_kmh_planned'] / 1000
        )

    # Fix trips ending after midnight
    required_cols = [
        'departure_time_planned',
        'arrival_time_planned',
        'duration_min_planned',
        'speed_kmh_planned',
        'route_length'
    ]

    if all(col in df.columns for col in required_cols):

        df['departure_time_planned'] = pd.to_datetime(
            df['departure_time_planned'].astype(str),
            format='mixed'
        )

        df['arrival_time_planned'] = pd.to_datetime(
            df['arrival_time_planned'].astype(str),
            format='mixed'
        )

        mask_midnight = df['arrival_time_planned'] < df['departure_time_planned']

        if verbose:
            print(f"Trips ending after midnight: {mask_midnight.sum()}")

        df.loc[mask_midnight, 'duration_min_planned'] = (
            (
                df.loc[mask_midnight, 'arrival_time_planned'] + pd.Timedelta(days=1)
            ) -
            df.loc[mask_midnight, 'departure_time_planned']
        ).dt.total_seconds() / 60

        df.loc[mask_midnight, 'speed_kmh_planned'] = (
            (df.loc[mask_midnight, 'route_length'] / 1000) /
            (df.loc[mask_midnight, 'duration_min_planned'] / 60)
        )

    # Optional: remove target outliers
    if remove_target_outliers and 'duration_difference_min' in df.columns:
        before = len(df)

        df = df[
            (df['duration_difference_min'] >= -120) &
            (df['duration_difference_min'] <= 120)
        ].copy()

        after = len(df)

        if verbose:
            print(f"Rows removed: {before - after:,} ({(before - after)/before*100:.2f}%)")
            print(f"Rows remaining: {after:,}")

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

    df_copy['is_night'] = df_copy['full_hour'].isin([0,1,2,3,4,5]).astype(int)
    df_copy['night_x_long_route'] = df_copy['is_night'] * (df['route_length']/1000)
    

    return df_copy

def encode_categorical_columns(df, te=None, target_col="delay_cat"):
    """
    Encode categorical columns.

    For train:
        df, te = encode_categorical_columns(train_df)

    For validation/test:
        df, _ = encode_categorical_columns(val_df, te=te)
    """

    import pandas as pd
    from category_encoders import TargetEncoder

    df = df.copy()

    # Remove duplicated columns
    df = df.loc[:, ~df.columns.duplicated()]

    # -----------------------------
    # Ordinal encoding - day
    # -----------------------------
    day_order = [
        'Sunday', 'Monday', 'Tuesday', 'Wednesday',
        'Thursday', 'Friday', 'Saturday'
    ]

    day_mapping = {day: i for i, day in enumerate(day_order)}

    df['day_encoded'] = df['day'].map(day_mapping)

    # -----------------------------
    # Ordinal encoding - route_length_bin
    # -----------------------------
    route_length_mapping = {
        '0-30k': 0,
        '30-100k': 1,
        '100k+': 2
    }

    if 'route_length_bin' in df.columns:
        df['route_length_bin_encoded'] = (
            df['route_length_bin']
            .map(route_length_mapping)
            .astype(int)
        )

    # -----------------------------
    # Target encoding
    # -----------------------------
    target_cols = [
        'agency_name',
        'origin_city',
        'destination_city',
        'origin_station',
        'destination_station',
        'agency_linenum_dir_alter'
    ]

    # Keep only columns that exist
    target_cols = [col for col in target_cols if col in df.columns]

    encoded_cols = [f'{col}_encoded' for col in target_cols]

    if te is None:
        te = TargetEncoder()

        df[encoded_cols] = te.fit_transform(
            df[target_cols],
            df[target_col]
        )

    else:
        df[encoded_cols] = te.transform(
            df[target_cols]
        )

    # Remove duplicated columns again
    df = df.loc[:, ~df.columns.duplicated()]

    return df, te




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
        'origin_station', 'destination_city', 'destination_station','route_length_bin',
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

def fill_planned_missing_values(df, verbose=True):
    import pandas as pd
    
    df = df.copy()

    # Convert to datetime
    df['departure_time_planned'] = pd.to_datetime(
        df['departure_time_planned'].astype(str), format='%H:%M:%S', errors='coerce'
    )
    df['arrival_time_planned'] = pd.to_datetime(
        df['arrival_time_planned'].astype(str), format='%H:%M:%S', errors='coerce'
    )

    selected_cols = [
        'departure_time_planned',
        'arrival_time_planned',
        'duration_min_planned',
        'duration_min_actual',
        'speed_kmh_planned',
        'speed_kmh_actual',
        'duration_difference_min'
    ]

    # --- Fill duration_min_planned ---
    mask1 = df['duration_min_planned'].isna()
    df.loc[mask1, 'duration_min_planned'] = (
        (df.loc[mask1, 'arrival_time_planned'] - df.loc[mask1, 'departure_time_planned'])
        .dt.total_seconds() / 60
    )
    if verbose:
        print(f"duration_min_planned filled: {mask1.sum()}")

    # --- Fill duration_difference_min ---
    mask2 = df['duration_difference_min'].isna()
    df.loc[mask2, 'duration_difference_min'] = (
        df.loc[mask2, 'duration_min_actual'] - df.loc[mask2, 'duration_min_planned']
    )
    if verbose:
        print(f"duration_difference_min filled: {mask2.sum()}")

    # --- Fill speed_kmh_planned ---
    mask3 = df['speed_kmh_planned'].isna()
    df.loc[mask3, 'speed_kmh_planned'] = (
        (df.loc[mask3, 'route_length'] / 1000) /
        (df.loc[mask3, 'duration_min_planned'] / 60)
    )
    if verbose:
        print(f"speed_kmh_planned filled: {mask3.sum()}")

    # --- Remove invalid values ---
    if verbose:
        print(f"duration_min_planned < 0: {(df['duration_min_planned'] < 0).sum():,}")
        print(f"speed_kmh_planned < 0:    {(df['speed_kmh_planned'] < 0).sum():,}")

    df = df[
        (df['duration_min_planned'] >= 0) &
        (df['speed_kmh_planned'] >= 0)
    ]

    if verbose:
        print(f"\nRows after drop: {len(df):,}")

    # --- Missing summary ---
    missing_summary = pd.DataFrame({
        'missing_count': df.isnull().sum(),
        'missing_percent': df.isnull().mean() * 100
    }).sort_values(by='missing_percent', ascending=False)

    if verbose:
        display(df[selected_cols].head())
        display(missing_summary)

    return df, missing_summary



def add_probability_features(
    df,
    ref_df=None,
    bins=[0, 30000, 100000, float('inf')],
    labels=['0-30k', '30-100k', '100k+'],
    smoothing_alpha=0
):
    import pandas as pd
    import numpy as np

    df = df.copy()

    if ref_df is None:
        ref_df = df.copy()
    else:
        ref_df = ref_df.copy()

    # Create route length bins in both df and ref_df
    df['route_length_bin'] = pd.cut(
        df['route_length'],
        bins=bins,
        labels=labels,
        right=False
    )

    ref_df['route_length_bin'] = pd.cut(
        ref_df['route_length'],
        bins=bins,
        labels=labels,
        right=False
    )

    # Build probability table from ref_df only
    counts = (
        ref_df
        .groupby(['full_hour', 'route_length_bin', 'delay_cat'], observed=True)
        .size()
        .unstack(fill_value=0)
    )

    if smoothing_alpha > 0:
        global_dist = ref_df['delay_cat'].value_counts(normalize=True)
        counts = counts + smoothing_alpha * global_dist

    prob_table = counts.div(counts.sum(axis=1), axis=0)

    # Make sure early column exists
    if 'early' not in prob_table.columns:
        prob_table['early'] = 0

    early_prob = prob_table['early'].reset_index()

    # Merge early probability to df
    df = df.merge(
        early_prob,
        on=['full_hour', 'route_length_bin'],
        how='left'
    )

    df = df.rename(columns={'early': 'early_by_hour_length_proba'})

    # Fill missing with global early rate from ref_df
    global_early_rate = (ref_df['delay_cat'] == 'early').mean()

    df['early_by_hour_length_proba'] = (
        df['early_by_hour_length_proba']
        .fillna(global_early_rate)
    )

    return df


def create_target_column(
    df,
    source_col="duration_difference_min",
    target_col="target",
    early_threshold=-10,
    delay_threshold=10,
    print_distribution=True
):
    """
    Create a categorical target column based on numeric thresholds.

    Parameters:
    df : DataFrame
    source_col : str
        Column with numeric values (e.g., duration difference)
    target_col : str
        Name of the new target column
    early_threshold : float
        Values below this are 'early'
    delay_threshold : float
        Values above this are 'delay'
    print_distribution : bool
        Whether to print value counts

    Returns:
    df : DataFrame (with new column)
    """

    import numpy as np

    conditions = [
        df[source_col] < early_threshold,
        df[source_col] >= delay_threshold
    ]

    choices = ["early", "delay"]

    df[target_col] = np.select(conditions, choices, default="on_time")

    if print_distribution:
        print(f"\nDistribution of '{target_col}':")
        print(df[target_col].value_counts())
        print(df[target_col].value_counts(normalize=True))

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


def run_feature_selection_methods_classification(X_train, y_train):
    """
    Run multiple feature selection methods for a classification problem.

    Returns:
    selection_df : DataFrame with selected/not selected features per method
    """

    import numpy as np
    import pandas as pd

    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.svm import LinearSVC
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    methods = [
        "RandomForest",
        "GradientBoost",
        "LinearSVC",
        "LogisticRegression_L2"
    ]

    print("Feature selection methods to be used:")
    for i, method in enumerate(methods, start=1):
        print(f"{i}. {method}")

    print("-" * 50)

    total_methods = len(methods)
    finished = 0

    # 1. Random Forest
    rf = RandomForestClassifier(
        random_state=42,
        class_weight="balanced"
    ).fit(X_train, y_train)

    rf_selected = (rf.feature_importances_ > 0).astype(int)

    finished += 1
    print(f"Finished RandomForest ({finished}/{total_methods})")

    # 2. Gradient Boosting
    gb = GradientBoostingClassifier(
        random_state=42
    ).fit(X_train, y_train)

    gb_selected = (gb.feature_importances_ > 0).astype(int)

    finished += 1
    print(f"Finished GradientBoost ({finished}/{total_methods})")

    # 3. Linear SVC
    svc = make_pipeline(
        StandardScaler(),
        LinearSVC(
            C=0.01,
            max_iter=10000,
            class_weight="balanced",
            random_state=42
        )
    )

    svc.fit(X_train, y_train)

    svc_coef = svc.named_steps["linearsvc"].coef_
    svc_selected = (np.abs(svc_coef).sum(axis=0) > 0).astype(int)

    finished += 1
    print(f"Finished LinearSVC ({finished}/{total_methods})")

    # 4. Logistic Regression with L2
    log_l2 = make_pipeline(
        StandardScaler(),
        LogisticRegression(
            penalty="l2",
            solver="lbfgs",
            max_iter=10000,
            class_weight="balanced",
            random_state=42
        )
    )

    log_l2.fit(X_train, y_train)

    log_l2_coef = log_l2.named_steps["logisticregression"].coef_
    log_l2_selected = (np.abs(log_l2_coef).sum(axis=0) > 0).astype(int)

    finished += 1
    print(f"Finished LogisticRegression_L2 ({finished}/{total_methods})")

    # Create results DataFrame
    selection_df = pd.DataFrame({
        "Feature": X_train.columns,
        "LinearSVC": svc_selected,
        "GradientBoost": gb_selected,
        "RandomForest": rf_selected,
        "Logistic_L2": log_l2_selected
    })

    selection_cols = [
        "LinearSVC",
        "GradientBoost",
        "RandomForest",
        "Logistic_L2"
    ]

    selection_df["Sum"] = selection_df[selection_cols].sum(axis=1)

    selection_df = selection_df.sort_values(
        by="Sum",
        ascending=False
    ).reset_index(drop=True)

    return selection_df


def test_xgboost_by_feature_votes_split(
    X_train,
    X_test,
    y_train,
    y_test,
    selection_df,
    vote_options=[4, 3]
):
    results = []
    models = {}

    le = LabelEncoder()
    y_train_enc = le.fit_transform(y_train)
    y_test_enc = le.transform(y_test)

    for min_votes in vote_options:

        selected_features = selection_df.loc[
            selection_df["Sum"] >= min_votes,
            "Feature"
        ].tolist()

        X_train_selected = X_train[selected_features]
        X_test_selected = X_test[selected_features]

        model = XGBClassifier(
            objective="multi:softprob",
            eval_metric="mlogloss",
            random_state=42,
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8
        )

        model.fit(X_train_selected, y_train_enc)

        y_pred = model.predict(X_test_selected)

        results.append({
            "min_votes": min_votes,
            "n_features": len(selected_features),
            "accuracy": accuracy_score(y_test_enc, y_pred),
            "macro_f1": f1_score(y_test_enc, y_pred, average="macro"),
            "weighted_f1": f1_score(y_test_enc, y_pred, average="weighted")
        })

        models[min_votes] = model

        print("=" * 60)
        print(f"Features with at least {min_votes} votes")
        print(f"Number of features: {len(selected_features)}")
        print(classification_report(
            y_test_enc,
            y_pred,
            target_names=le.classes_
        ))

    return pd.DataFrame(results), models


def manipulate_df_process(df, ref_df=None, train=True, te=None):
    """
    Full preprocessing process for train / validation / test.

    Train:
        df, te = manipulate_df_process(train_df, train=True)

    Validation/Test:
        df = manipulate_df_process(val_df, ref_df=train_processed_df, train=False, te=te)
    """

    df = df.copy()

    # 1. Fix data types
    df = fix_data_types(df)

    # 2. Handle outliers
    df = handle_outliers(df)

    # 3. Handle missing values
    if train:
        df = handle_missing_values(df)
    else:
        df = handle_missing_values(df, ref_df=ref_df)

    # 4. Add row-based features
    df = add_features(df)

    # 5. Add probability features
    if train:
        df = add_probability_features(df)
    else:
        df = add_probability_features(df, ref_df=ref_df)

    # 6. Encode categorical columns
    if train:
        df, te = encode_categorical_columns(df, te=None)
        return df, te

    else:
        df, _ = encode_categorical_columns(df, te=te)
        return df
