"""
Sinh dữ liệu thời tiết giả lập gửi đến Azure Event Hubs.
Dữ liệu thời tiết ảnh hưởng đến dự đoán bán hàng.
"""

import json
import random
import time
import signal
import sys
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

sys.path.insert(0, ".")
from config.settings import (
    EVENT_HUB_CONNECTION_STRING,
    REGIONS,
)

# Event Hub riêng cho dữ liệu thời tiết
WEATHER_EVENT_HUB_NAME = "weather-events"


class WeatherDataGenerator:
    """Sinh dữ liệu thời tiết giả lập theo từng khu vực."""

    WEATHER_CONDITIONS = ["Sunny", "Cloudy", "Rainy", "Stormy", "Snowy", "Windy", "Foggy"]

    # Nhiệt độ cơ sở theo vùng (°C)
    REGION_TEMP_BASE = {
        "North": 15,
        "South": 30,
        "East": 22,
        "West": 25,
        "Central": 20,
    }

    def __init__(self):
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STRING,
            eventhub_name=WEATHER_EVENT_HUB_NAME,
        )
        self.running = True
        self.total_sent = 0
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        print(f"\n[INFO] Đang dừng... Đã gửi tổng cộng {self.total_sent} bản ghi thời tiết.")
        self.running = False

    def generate_weather_event(self, region: str) -> dict:
        """Tạo dữ liệu thời tiết cho một khu vực."""
        now = datetime.now(timezone.utc)
        base_temp = self.REGION_TEMP_BASE.get(region, 20)

        # Biến động nhiệt độ theo giờ
        hour_variation = -5 + 10 * abs(now.hour - 12) / 12  # lạnh hơn vào đêm
        temperature = round(base_temp + random.uniform(-5, 5) - hour_variation, 1)

        condition = random.choice(self.WEATHER_CONDITIONS)
        humidity = random.randint(30, 95)
        wind_speed = round(random.uniform(0, 50), 1)

        # Lượng mưa phụ thuộc vào điều kiện thời tiết
        precipitation = 0.0
        if condition in ["Rainy", "Stormy"]:
            precipitation = round(random.uniform(1, 50), 1)
        elif condition == "Snowy":
            precipitation = round(random.uniform(0.5, 20), 1)

        return {
            "timestamp": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "hour": now.hour,
            "region": region,
            "temperature_celsius": temperature,
            "humidity_percent": humidity,
            "wind_speed_kmh": wind_speed,
            "precipitation_mm": precipitation,
            "weather_condition": condition,
            "uv_index": random.randint(0, 11) if condition == "Sunny" else random.randint(0, 4),
        }

    def send_weather_data(self):
        """Gửi dữ liệu thời tiết cho tất cả các vùng."""
        event_data_batch = self.producer.create_batch()
        count = 0

        for region in REGIONS:
            event = self.generate_weather_event(region)
            event_json = json.dumps(event)
            try:
                event_data_batch.add(EventData(event_json))
                count += 1
            except ValueError:
                break

        self.producer.send_batch(event_data_batch)
        self.total_sent += count
        return count

    def run(self, interval: float = 30.0):
        """Chạy vòng lặp sinh dữ liệu thời tiết (mỗi 30 giây)."""
        print("=" * 60)
        print("  HỆ THỐNG SINH DỮ LIỆU THỜI TIẾT THỜI GIAN THỰC")
        print(f"  Event Hub: {WEATHER_EVENT_HUB_NAME}")
        print(f"  Regions: {', '.join(REGIONS)}")
        print(f"  Interval: {interval}s")
        print("  Nhấn Ctrl+C để dừng")
        print("=" * 60)

        try:
            while self.running:
                count = self.send_weather_data()
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] Đã gửi {count} bản ghi thời tiết | Tổng: {self.total_sent}")
                time.sleep(interval)
        except Exception as e:
            print(f"[ERROR] Lỗi: {e}")
        finally:
            self.producer.close()
            print(f"[INFO] Đã đóng kết nối. Tổng bản ghi thời tiết: {self.total_sent}")


def main():
    generator = WeatherDataGenerator()
    generator.run()


if __name__ == "__main__":
    main()
