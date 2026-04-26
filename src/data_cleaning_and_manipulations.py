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