import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import LineString, MultiLineString, Point
import re


#### Weather related functions #######


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



##### Spatial Attributes #######


def route_to_shape_dict(df_trips, route_ids):
    filtered = df_trips[df_trips["route_id"].isin(route_ids)].copy()

    route_shape = (
        filtered[["route_id", "shape_id"]]
        .drop_duplicates(subset=["route_id"])
        .set_index("route_id")["shape_id"]
        .apply(lambda x: {"shape_id": x})
        .to_dict()
    )

    return route_shape

def get_linestring_for_shape(shapes_df, shape_id):
    df = shapes_df[shapes_df["shape_id"] == shape_id].copy()

    # sort by sequence
    df = df.sort_values("shape_pt_sequence")

    # build LineString from lon, lat
    line = LineString(zip(df["shape_pt_lon"], df["shape_pt_lat"]))

    return line


def add_linestrings_to_route_dict(shapes_df, route_shape_dict):
    """
    Adds a LineString to each route entry in the route->shape dictionary.

    Input example:
    {
        2259: {"shape_id": 12345},
        2260: {"shape_id": 67890}
    }

    Output example:
    {
        2259: {"shape_id": 12345, "linestring": <LineString>},
        2260: {"shape_id": 67890, "linestring": <LineString>}
    }
    """
    result = route_shape_dict.copy()

    for route_id, values in result.items():
        shape_id = values["shape_id"]

        df_shape = shapes_df[shapes_df["shape_id"] == shape_id].copy()
        df_shape = df_shape.sort_values("shape_pt_sequence")

        line = LineString(zip(df_shape["shape_pt_lon"], df_shape["shape_pt_lat"]))

        result[route_id]["linestring"] = line

    return result


def extract_linestring(route_id, route_dict):
    return route_dict.get(route_id, {}).get("linestring", None)

def buffer_and_dissolve_routes(routes_gdf, buffer_m=25):
    """
    Buffers all geometries by buffer_m and dissolves them into one geometry.

    Parameters
    ----------
    routes_gdf : GeoDataFrame
        Input routes GeoDataFrame in a projected CRS (meters), e.g. EPSG:2039.
    buffer_m : float
        Buffer distance in meters.

    Returns
    -------
    GeoDataFrame
        One-row GeoDataFrame with dissolved buffered geometry.
    """
    gdf = routes_gdf.copy()

    gdf["geometry"] = gdf.geometry.buffer(buffer_m)

    dissolved = gdf.dissolve()

    return dissolved



def calc_length_within_buffer(route_geom, buffer_geom, min_length=100):
    """
    Returns length (in meters) of route inside buffer,
    summing only segments longer than min_length.
    """

    if route_geom is None or route_geom.is_empty:
        return None

    intersection = route_geom.intersection(buffer_geom)

    if intersection.is_empty:
        return 0

    total_length = 0

    # Case 1: single LineString
    if isinstance(intersection, LineString):
        if intersection.length >= min_length:
            total_length += intersection.length

    # Case 2: MultiLineString (most common case)
    elif isinstance(intersection, MultiLineString):
        for part in intersection.geoms:
            if part.length >= min_length:
                total_length += part.length

    return total_length




def calc_curvity(line):
    """
    Calculate route curvity/sinuosity:
    route length / straight-line distance

    Parameters
    ----------
    line : shapely LineString

    Returns
    -------
    float or None
    """
    if line is None or line.is_empty:
        return None

    coords = list(line.coords)
    if len(coords) < 2:
        return None

    start = Point(coords[0])
    end = Point(coords[-1])

    straight_dist = start.distance(end)
    if straight_dist == 0:
        return None

    return line.length / straight_dist


def parse_mixed_date(value):
    """
    Parse mixed date formats into pandas Timestamp.

    Rules:
    - '2023'       -> 01/07/2023
    - '2019_Q1'    -> 01/04/2019
    - '08/2025'    -> 01/08/2025
    - 'dd/mm/yyyy' -> parsed as-is
    """
    if pd.isna(value):
        return pd.NaT

    s = str(value).strip()

    # dd/mm/yyyy
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")

    # yyyy
    if re.fullmatch(r"\d{4}", s):
        return pd.Timestamp(year=int(s), month=7, day=1)

    # yyyy_Qn
    m = re.fullmatch(r"(\d{4})_Q([1-4])", s)
    if m:
        year = int(m.group(1))
        quarter = int(m.group(2))
        quarter_to_month = {
            1: 4,
            2: 7,
            3: 10,
            4: 1
        }
        month = quarter_to_month[quarter]
        # for Q4, month 1 should be in same year per your rule style
        return pd.Timestamp(year=year, month=month, day=1)

    # mm/yyyy
    m = re.fullmatch(r"(\d{2})/(\d{4})", s)
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        return pd.Timestamp(year=year, month=month, day=1)

    return pd.NaT


def filter_by_multiple_date_windows(df, start_cols, end_cols, start_cutoffs, end_cutoffs):
    """
    Filters rows based on multiple (start, end) date column pairs.

    Each pair must satisfy:
    - start_col < start_cutoff OR is NaT
    - end_col > end_cutoff OR is NaT

    All conditions are combined with AND.

    Parameters
    ----------
    df : DataFrame
    start_cols : list of str
    end_cols : list of str
    start_cutoffs : list of str or Timestamp
    end_cutoffs : list of str or Timestamp

    Returns
    -------
    DataFrame
    """

    if not (len(start_cols) == len(end_cols) == len(start_cutoffs) == len(end_cutoffs)):
        raise ValueError("All input lists must have the same length")

    mask = pd.Series(True, index=df.index)

    for sc, ec, scut, ecut in zip(start_cols, end_cols, start_cutoffs, end_cutoffs):
        scut = pd.to_datetime(scut)
        ecut = pd.to_datetime(ecut)

        cond = (
            (df[sc].isna() | (df[sc] < scut)) &
            (df[ec].isna() | (df[ec] > ecut))
        )

        mask &= cond

    return df[mask]


