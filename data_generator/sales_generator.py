from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone
import os
import sys
import requests
from azure.eventhub import EventData, EventHubProducerClient

# Thêm thư mục gốc vào path để import config
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import settings

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("sales_generator")

WEATHER_CACHE: dict = {}
HOLIDAY_CACHE: dict = {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_today_parts() -> tuple[int, int, int]:
    now = datetime.now(timezone.utc)
    return now.year, now.month, now.day


def choose_store_id() -> str:
    return random.choice(list(settings.STORE_LOCATIONS.keys()))


def choose_product() -> dict:
    return random.choice(settings.SALES_PRODUCTS)


def random_quantity() -> int:
    return random.randint(1, 5)


def random_price(product: dict) -> float:
    min_price = product["min_price"]
    max_price = product["max_price"]
    return round(random.uniform(min_price, max_price), 2)


def normalize_weather_condition(raw_weather: str) -> str:
    if not raw_weather:
        return "unknown"

    value = raw_weather.strip().lower()
    mapping = {
        "clear": "sunny",
        "clouds": "cloudy",
        "rain": "rainy",
        "drizzle": "rainy",
        "thunderstorm": "stormy",
        "mist": "foggy",
        "fog": "foggy",
        "haze": "foggy",
        "smoke": "foggy",
    }
    return mapping.get(value, "unknown")


def fetch_weather_from_api(store_id: str) -> dict:
    api_key = settings.OPENWEATHER_API_KEY
    if not api_key:
        raise ValueError("Thiếu OPENWEATHER_API_KEY trong settings")

    location = settings.STORE_LOCATIONS.get(store_id)
    if not location:
        raise ValueError(f"Không tìm thấy STORE_LOCATIONS cho store_id={store_id}")

    url = "https://api.openweathermap.org/data/2.5/weather"

    if isinstance(location, dict):
        lat = location.get("lat")
        lon = location.get("lon")
        name = location.get("name")

        if lat is not None and lon is not None:
            params = {
                "lat": lat,
                "lon": lon,
                "appid": api_key,
                "units": "metric",
                "lang": "en",
            }
        elif name:
            params = {
                "q": f"{name},VN",
                "appid": api_key,
                "units": "metric",
                "lang": "en",
            }
        else:
            raise ValueError(f"STORE_LOCATIONS[{store_id}] thiếu name hoặc lat/lon")
    else:
        params = {
            "q": f"{location},VN",
            "appid": api_key,
            "units": "metric",
            "lang": "en",
        }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    temp = data.get("main", {}).get("temp")
    weather_items = data.get("weather", [])
    weather_main = weather_items[0].get("main") if weather_items else None

    if temp is None:
        raise ValueError(f"API không trả về nhiệt độ cho store_id={store_id}")

    return {
        "temperature": int(round(temp)),
        "weather": normalize_weather_condition(weather_main),
    }


def get_weather_for_store(store_id: str) -> dict:
    now_ts = time.time()
    cached = WEATHER_CACHE.get(store_id)

    if cached and cached["expires_at"] > now_ts:
        logger.info("weather_cache_hit store_id=%s", store_id)
        return cached["data"]

    logger.info("weather_cache_miss store_id=%s", store_id)

    try:
        weather_data = fetch_weather_from_api(store_id)
        WEATHER_CACHE[store_id] = {
            "expires_at": now_ts + settings.WEATHER_CACHE_TTL,
            "data": weather_data,
        }
        return weather_data
    except Exception as exc:
        logger.warning(
            "weather_api_failed store_id=%s error=%s",
            store_id,
            exc,
        )

        if cached:
            logger.warning("weather_fallback_to_cached store_id=%s", store_id)
            return cached["data"]

        logger.warning("weather_fallback_to_default store_id=%s", store_id)

        fallback_data = {
            "temperature": 30,
            "weather": "unknown",
        }
        WEATHER_CACHE[store_id] = {
            "expires_at": now_ts + settings.WEATHER_CACHE_TTL,
            "data": fallback_data,
        }
        return fallback_data


def fetch_holiday_from_api(country_code: str, year: int, month: int, day: int) -> int:
    api_key = settings.CALENDARIFIC_API_KEY
    if not api_key:
        raise ValueError("Thiếu CALENDARIFIC_API_KEY trong settings")

    url = "https://calendarific.com/api/v2/holidays"
    params = {
        "api_key": api_key,
        "country": country_code,
        "year": year,
        "month": month,
        "day": day,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    holidays = data.get("response", {}).get("holidays", [])
    return 1 if holidays else 0


def get_holiday_flag() -> int:
    year, month, day = utc_today_parts()
    country_code = settings.DEFAULT_COUNTRY_CODE
    cache_key = f"{country_code}|{year:04d}-{month:02d}-{day:02d}"

    now_ts = time.time()
    cached = HOLIDAY_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now_ts:
        logger.info("holiday_cache_hit cache_key=%s", cache_key)
        return cached["data"]

    logger.info("holiday_cache_miss cache_key=%s", cache_key)

    try:
        holiday_flag = fetch_holiday_from_api(country_code, year, month, day)
        HOLIDAY_CACHE[cache_key] = {
            "expires_at": now_ts + settings.HOLIDAY_CACHE_TTL,
            "data": holiday_flag,
        }
        return holiday_flag
    except Exception as exc:
        logger.warning(
            "holiday_api_failed cache_key=%s error=%s",
            cache_key,
            exc,
        )

        if cached:
            logger.warning("holiday_fallback_to_cached cache_key=%s", cache_key)
            return cached["data"]

        logger.warning("holiday_fallback_to_default cache_key=%s", cache_key)

        fallback_value = 0
        HOLIDAY_CACHE[cache_key] = {
            "expires_at": now_ts + settings.HOLIDAY_CACHE_TTL,
            "data": fallback_value,
        }
        return fallback_value


def build_sales_event() -> dict:
    store_id = choose_store_id()
    product = choose_product()
    weather_data = get_weather_for_store(store_id)
    holiday_flag = get_holiday_flag()

    event = {
        "timestamp": utc_now_iso(),
        "store_id": store_id,
        "product_id": product["product_id"],
        "quantity": random_quantity(),
        "price": random_price(product),
        "temperature": weather_data["temperature"],
        "weather": weather_data["weather"],
        "holiday": holiday_flag,
    }
    return event


def validate_event_schema(event: dict) -> None:
    required_fields = {
        "timestamp": str,
        "store_id": str,
        "product_id": str,
        "quantity": int,
        "price": (int, float),
        "temperature": (int, float),
        "weather": str,
        "holiday": int,
    }

    missing = [field for field in required_fields if field not in event]
    if missing:
        raise ValueError(f"Thiếu field bắt buộc: {missing}")

    for field, expected_type in required_fields.items():
        if not isinstance(event[field], expected_type):
            raise TypeError(
                f"Field {field} có kiểu {type(event[field])}, expected {expected_type}"
            )

    if event["holiday"] not in (0, 1):
        raise ValueError("Field holiday chỉ được nhận 0 hoặc 1")

    if event["quantity"] <= 0:
        raise ValueError("Field quantity phải > 0")

    if event["price"] <= 0:
        raise ValueError("Field price phải > 0")


def build_dedupe_key(event: dict) -> str:
    parts = [
        str(event["timestamp"]),
        str(event["store_id"]),
        str(event["product_id"]),
        str(event["quantity"]),
        str(event["price"]),
    ]
    return "|".join(parts)


def generate_batch(batch_size: int) -> list[dict]:
    if batch_size <= 0:
        raise ValueError("batch_size phải > 0")

    events = []
    for _ in range(batch_size):
        event = build_sales_event()
        validate_event_schema(event)
        events.append(event)
    return events


def print_sample_events(events: list[dict], mode: str) -> None:
    for index, event in enumerate(events, start=1):
        dedupe_key = build_dedupe_key(event)
        logger.info(
            "%s_event_generated index=%s store_id=%s product_id=%s dedupe_key=%s",
            mode,
            index,
            event["store_id"],
            event["product_id"],
            dedupe_key,
        )
        print(json.dumps(event, ensure_ascii=False))


def write_events_to_jsonl(events: list[dict], file_path: str, append: bool = True) -> None:
    file_mode = "a" if append else "w"
    with open(file_path, file_mode, encoding="utf-8") as file_obj:
        for event in events:
            file_obj.write(json.dumps(event, ensure_ascii=False) + "\n")


def get_effective_rate_per_minute(started_at: float) -> int:
    base_rate = max(1, settings.RATE_PER_MINUTE)

    if settings.BURST_ENABLED:
        elapsed = time.time() - started_at
        if elapsed < settings.BURST_DURATION_SECONDS:
            return max(1, base_rate * max(1, settings.BURST_MULTIPLIER))

    return base_rate


def calculate_events_per_cycle(rate_per_minute: int, interval_seconds: float) -> int:
    if interval_seconds <= 0:
        raise ValueError("SALES_GENERATION_INTERVAL phải > 0")

    events_per_cycle = int(round(rate_per_minute * interval_seconds / 60.0))
    return max(1, events_per_cycle)


def load_replay_events(file_path: str) -> list[dict]:
    if not os.path.exists(file_path):
        logger.warning("replay_file_not_found replay_file=%s", file_path)
        return []

    events = []
    with open(file_path, "r", encoding="utf-8") as file_obj:
        for line_number, raw_line in enumerate(file_obj, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "replay_line_invalid_json line_number=%s error=%s",
                    line_number,
                    exc,
                )
                continue

            try:
                validate_event_schema(event)
            except Exception as exc:
                logger.warning(
                    "replay_line_invalid_schema line_number=%s error=%s",
                    line_number,
                    exc,
                )
                continue

            events.append(event)

    logger.info("replay_events_loaded total=%s replay_file=%s", len(events), file_path)
    return events


def create_eventhub_producer() -> EventHubProducerClient:
    settings.validate_required_settings(mode="eventhub")
    producer = EventHubProducerClient.from_connection_string(
        conn_str=settings.EVENT_HUB_CONNECTION_STRING
    )
    return producer


def to_event_data(event: dict) -> EventData:
    validate_event_schema(event)
    payload = json.dumps(event, ensure_ascii=False)
    return EventData(payload)


def group_events_by_store(events: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for event in events:
        store_id = event["store_id"]
        grouped.setdefault(store_id, []).append(event)
    return grouped


def send_events_with_retry(producer: EventHubProducerClient, events: list[dict]) -> None:
    grouped = group_events_by_store(events)

    for store_id, store_events in grouped.items():
        attempt = 0

        while True:
            attempt += 1
            try:
                batch = producer.create_batch(partition_key=store_id)
                batch_count = 0
                total_sent = 0

                for event in store_events:
                    event_data = to_event_data(event)

                    try:
                        batch.add(event_data)
                        batch_count += 1
                    except ValueError:
                        producer.send_batch(batch)
                        total_sent += batch_count

                        logger.info(
                            "eventhub_batch_sent store_id=%s batch_size=%s total_sent=%s",
                            store_id,
                            batch_count,
                            total_sent,
                        )

                        batch = producer.create_batch(partition_key=store_id)
                        batch.add(event_data)
                        batch_count = 1

                if batch_count > 0:
                    producer.send_batch(batch)
                    total_sent += batch_count

                    logger.info(
                        "eventhub_batch_sent store_id=%s batch_size=%s total_sent=%s",
                        store_id,
                        batch_count,
                        total_sent,
                    )

                logger.info(
                    "eventhub_send_completed store_id=%s total=%s attempt=%s",
                    store_id,
                    len(store_events),
                    attempt,
                )
                break

            except Exception as exc:
                if attempt >= settings.EVENT_HUB_MAX_RETRIES:
                    logger.exception(
                        "eventhub_send_failed store_id=%s total=%s attempt=%s error=%s",
                        store_id,
                        len(store_events),
                        attempt,
                        exc,
                    )
                    raise

                backoff_seconds = min(
                    settings.EVENT_HUB_RETRY_BACKOFF_FACTOR * (2 ** (attempt - 1)),
                    settings.EVENT_HUB_RETRY_BACKOFF_MAX,
                )

                logger.warning(
                    "eventhub_send_retry store_id=%s total=%s attempt=%s backoff_seconds=%s error=%s",
                    store_id,
                    len(store_events),
                    attempt,
                    backoff_seconds,
                    exc,
                )
                time.sleep(backoff_seconds)


def run_live_mode() -> None:
    logger.info("starting_live_mode")
    settings.validate_required_settings(mode="eventhub")

    interval_seconds = settings.SALES_GENERATION_INTERVAL
    started_at = time.time()
    cycle_number = 0
    producer = create_eventhub_producer()

    try:
        while True:
            cycle_number += 1
            cycle_started_at = time.time()

            effective_rate = get_effective_rate_per_minute(started_at)
            batch_size = calculate_events_per_cycle(effective_rate, interval_seconds)

            logger.info(
                "live_mode_plan cycle=%s interval_seconds=%s configured_batch_size=%s "
                "base_rate_per_minute=%s effective_rate_per_minute=%s "
                "burst_enabled=%s burst_multiplier=%s burst_duration_seconds=%s "
                "cycle_batch_size=%s",
                cycle_number,
                interval_seconds,
                settings.BATCH_SIZE,
                settings.RATE_PER_MINUTE,
                effective_rate,
                settings.BURST_ENABLED,
                settings.BURST_MULTIPLIER,
                settings.BURST_DURATION_SECONDS,
                batch_size,
            )

            events = generate_batch(batch_size)
            print_sample_events(events[: min(5, len(events))], mode="live_sample")

            if cycle_number == 1:
                write_events_to_jsonl(events, settings.REPLAY_FILE, append=False)
                logger.info(
                    "sample_replay_file_written replay_file=%s total=%s",
                    settings.REPLAY_FILE,
                    len(events),
                )

            send_events_with_retry(producer, events)

            logger.info(
                "live_mode_cycle_completed cycle=%s total=%s",
                cycle_number,
                len(events),
            )

            elapsed = time.time() - cycle_started_at
            sleep_seconds = max(0.0, interval_seconds - elapsed)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        logger.info("generator_stopped_by_user mode=live cycles_completed=%s", cycle_number)
    finally:
        producer.close()


def run_replay_mode() -> None:
    logger.info("starting_replay_mode replay_file=%s", settings.REPLAY_FILE)
    settings.validate_required_settings(mode="eventhub")

    replay_events = load_replay_events(settings.REPLAY_FILE)
    if not replay_events:
        logger.warning(
            "replay_mode_no_events replay_file=%s fallback_to_live_mode=true",
            settings.REPLAY_FILE,
        )
        run_live_mode()
        return

    print_sample_events(replay_events[: min(5, len(replay_events))], mode="replay_sample")

    producer = create_eventhub_producer()
    try:
        send_events_with_retry(producer, replay_events)
    finally:
        producer.close()

    logger.info("replay_mode_completed total=%s", len(replay_events))


def main() -> None:
    logger.info("runtime_config=%s", settings.get_runtime_config())

    if settings.REPLAY_MODE:
        run_replay_mode()
    else:
        run_live_mode()


if __name__ == "__main__":
    main()
