import pandas as pd

def modify_rainfall_df_values(df):
    df["datetime"] = pd.to_datetime(
        df["datetime"],
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )

    df["hour_rounded"] = df["datetime"].dt.hour
    df["date"] = df["datetime"].dt.date

    return df


def group_rainfall_by_day_and_hour(df):
    df_grouped = (
        df
        .groupby(["date", "hour_rounded"])["rainfall_mm"]
        .sum()
        .reset_index()
    )

    df_grouped["hour_rounded"] = df_grouped["hour_rounded"].astype(int)

    return df_grouped


def merge_rain_to_trips_df(df_rain_grouped, trips_df):
    trips_df["date"] = pd.to_datetime(trips_df["date"]).dt.date
    trips_df["hour_rounded"] = trips_df["hour_rounded"].astype(int)

    df_merged = trips_df.merge(
        df_rain_grouped[
            ["date", "hour_rounded", "rainfall_mm"]
        ],
        on=["date", "hour_rounded"],
        how="left"
    )
    return df_merged