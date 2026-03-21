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
