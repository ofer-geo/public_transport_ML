import requests
import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime, timezone


BASE = "https://open-bus-stride-api.hasadna.org.il"


def fetch_with_retry(url, params, max_retries=5, timeout=120):
    """Fetch with automatic retries on timeout/connection issues."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.ReadTimeout, requests.ConnectionError):
            wait = (attempt + 1) * 5
            print(f"⚠️ Attempt {attempt + 1}/{max_retries} failed — waiting {wait}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"❌ Error: {e}")
            raise
    raise Exception(f"Failed after {max_retries} attempts")


def fetch_all_rides(start_date, end_date, city_name="תל אביב", limit=100):
    rides = []
    offset = 0

    params = {
        "limit": limit,
        "scheduled_start_time_from": f"{start_date}T00:00:00Z",
        "scheduled_start_time_to": f"{end_date}T23:59:59Z",
        "gtfs_route__route_long_name_contains": city_name,
    }

    # Get total count
    count_params = {**params, "limit": 1, "get_count": "true"}
    total = fetch_with_retry(f"{BASE}/siri_rides/list", count_params)
    print(f'Total rides to fetch: {total}')

    if total == 0:
        print("0%")
        print("\n✅ Total 0 rides")
        return rides

    next_percent_to_print = 1

    while True:
        params["offset"] = offset
        batch = fetch_with_retry(f"{BASE}/siri_rides/list", params)

        if not isinstance(batch, list) or not batch:
            break

        rides.extend(batch)

        # Print every 1%
        current_percent = int((len(rides) / total) * 100)
        while next_percent_to_print <= current_percent and next_percent_to_print <= 100:
            print(f"{next_percent_to_print}%", end=' ', flush=True)
            next_percent_to_print += 1

        if len(batch) < limit:
            break

        offset += len(batch)
        time.sleep(0.15)

    print(f"\n✅ Total {len(rides)} rides")
    return rides


def fetch_stops_for_ride(gtfs_ride_id):
    r = requests.get(
        f"{BASE}/gtfs_ride_stops/list",
        params={
            "limit": 200,
            "gtfs_ride_ids": gtfs_ride_id,
            "order_by": "stop_sequence"
        },
        timeout=30
    )
    return r.json() if r.ok else []


def haversine(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, sqrt, atan2
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))




def fmt_time(s):
    if not s: return ""
    return datetime.fromisoformat(s).strftime("%H:%M:%S")

def fmt_date(s):
    if not s: return ""
    return datetime.fromisoformat(s).strftime("%d/%m/%Y")

def day_he(s):
    if not s: return ""
    days = ["שני","שלישי","רביעי","חמישי","שישי","שבת","ראשון"]
    return days[datetime.fromisoformat(s).weekday()]

def round_hour(s):
    if not s: return ""
    return f"{datetime.fromisoformat(s).hour:02d}:00"

def dur_min(a, b):
    if not a or not b: return ""
    m = round((datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds() / 60)
    return m if 0 < m < 600 else ""