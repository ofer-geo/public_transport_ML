import time
from tqdm import tqdm
import requests
from src.config import BASE
from src.geo_utils import haversine

def fetch_with_retry(url, params, max_retries=5, timeout=120):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.ReadTimeout, requests.ConnectionError):
            wait = (attempt + 1) * 5
            print(f"  ⚠️ Attempt {attempt+1}/{max_retries} — waiting {wait}'...")
            time.sleep(wait)
        except Exception as e:
            print(f"  ❌ Error: {e}")
            raise
    raise Exception("Failed after all attempts")


def fetch_all_rides(FROM_DATE,FROM_HOUR,TO_DATE,TO_HOUR):
    params = {
        "limit": 100,
        "scheduled_start_time_from": f"{FROM_DATE}T{FROM_HOUR}:00:00Z" ,
        "scheduled_start_time_to": f"{TO_DATE}T{TO_HOUR}:59:59Z" ,
        "gtfs_route__route_long_name_contains": "תל אביב",
    }

    total = fetch_with_retry(
        f"{BASE}/siri_rides/list",
        {**params, "limit": 1, "get_count": "true"}
    )
    print(f'Total {total:,} rides')

    rides, offset = [], 0
    pbar = tqdm(total=total, unit="נסיעות")

    while True:
        params["offset"] = offset
        batch = fetch_with_retry(f"{BASE}/siri_rides/list", params)

        if not isinstance(batch, list) or not batch:
            break

        rides.extend(batch)
        pbar.update(len(batch))

        if len(batch) < 100:
            break

        offset += len(batch)
        time.sleep(0.15)

    pbar.close()
    print(f"✅ {len(rides):,} Rides pulled")
    return rides


def fetch_stops(gtfs_ride_id):
    d = fetch_with_retry(
        f"{BASE}/gtfs_ride_stops/list",
        {
            "limit": 200,
            "gtfs_ride_ids": gtfs_ride_id,
            "order_by": "stop_sequence",
        },
    )
    return d if isinstance(d, list) else []


def build_route_sample_map(rides):
    seen = {}
    for ride in rides:
        route_id = ride.get("gtfs_ride__gtfs_route_id")
        gtfs_ride_id = ride.get("gtfs_ride_id")

        if route_id and route_id not in seen and gtfs_ride_id:
            seen[route_id] = gtfs_ride_id

    return seen


def summarize_route_from_stops(stops):
    if not stops:
        return {
            "from_city": "",
            "from_stop": "",
            "to_city": "",
            "to_stop": "",
            "stop_count": 0,
            "dist_km": 0,
        }

    dist = sum(
        haversine(
            stops[i - 1]["gtfs_stop__lat"],
            stops[i - 1]["gtfs_stop__lon"],
            stops[i]["gtfs_stop__lat"],
            stops[i]["gtfs_stop__lon"],
        )
        for i in range(1, len(stops))
        if stops[i - 1].get("gtfs_stop__lat") is not None
        and stops[i - 1].get("gtfs_stop__lon") is not None
        and stops[i].get("gtfs_stop__lat") is not None
        and stops[i].get("gtfs_stop__lon") is not None
    )

    return {
        "from_city": stops[0].get("gtfs_stop__city", ""),
        "from_stop": stops[0].get("gtfs_stop__name", ""),
        "to_city": stops[-1].get("gtfs_stop__city", ""),
        "to_stop": stops[-1].get("gtfs_stop__name", ""),
        "stop_count": len(stops),
        "dist_km": round(dist, 3),
    }


def build_route_cache(rides, sleep_sec=0.15):
    seen = build_route_sample_map(rides)

    print(f"Pulling station to -{len(seen):,} routes unique...")
    route_cache = {}

    for route_id, gtfs_ride_id in tqdm(seen.items()):
        try:
            stops = fetch_stops(gtfs_ride_id)
            route_cache[route_id] = summarize_route_from_stops(stops)
        except Exception:
            route_cache[route_id] = {}

        time.sleep(sleep_sec)

    print(f"✅ cache ready: {len(route_cache):,} routes")
    return route_cache