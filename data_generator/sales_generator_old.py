"""
Bộ sinh dữ liệu bán hàng giả lập gửi đến Azure Event Hubs.
Mô phỏng các giao dịch bán hàng thời gian thực với nhiều thuộc tính khác nhau.
"""

import json
import random
import time
import uuid
import signal
import sys
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

# Thêm thư mục gốc vào path
sys.path.insert(0, ".")
from config.settings import (
    EVENT_HUB_CONNECTION_STRING,
    EVENT_HUB_NAME,
    PRODUCTS,
    REGIONS,
    PAYMENT_METHODS,
    CUSTOMER_SEGMENTS,
    SALES_GENERATION_INTERVAL,
    BATCH_SIZE,
)


class SalesDataGenerator:
    """Sinh dữ liệu bán hàng ngẫu nhiên và gửi đến Azure Event Hubs."""

    def __init__(self):
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STRING,
            eventhub_name=EVENT_HUB_NAME,
        )
        self.running = True
        self.total_sent = 0
        self._customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, 501)]

        # Xử lý tắt chương trình
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        """Tắt chương trình một cách an toàn."""
        print(f"\n[INFO] Đang dừng... Đã gửi tổng cộng {self.total_sent} sự kiện.")
        self.running = False

    def generate_sale_event(self) -> dict:
        """Tạo một sự kiện bán hàng ngẫu nhiên."""
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 10)

        # Biến động giá ngẫu nhiên (giảm giá hoặc tăng giá)
        price_variation = random.uniform(0.85, 1.15)
        unit_price = round(product["base_price"] * price_variation, 2)
        total_amount = round(unit_price * quantity, 2)

        # Tính discount ngẫu nhiên
        discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20, 25])
        discount_amount = round(total_amount * discount_pct / 100, 2)
        final_amount = round(total_amount - discount_amount, 2)

        # Thời gian hiện tại (UTC)
        now = datetime.now(timezone.utc)

        event = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "hour": now.hour,
            "day_of_week": now.strftime("%A"),
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "discount_percent": discount_pct,
            "discount_amount": discount_amount,
            "final_amount": final_amount,
            "customer_id": random.choice(self._customer_ids),
            "customer_segment": random.choice(CUSTOMER_SEGMENTS),
            "region": random.choice(REGIONS),
            "payment_method": random.choice(PAYMENT_METHODS),
            "is_online": random.choice([True, False]),
            "rating": random.randint(1, 5) if random.random() > 0.3 else None,
        }
        return event

    def send_batch(self, batch_size: int = BATCH_SIZE):
        """Tạo và gửi một batch sự kiện đến Event Hub."""
        event_data_batch = self.producer.create_batch()
        events_in_batch = 0

        for _ in range(batch_size):
            event = self.generate_sale_event()
            event_json = json.dumps(event)
            try:
                event_data_batch.add(EventData(event_json))
                events_in_batch += 1
            except ValueError:
                # Batch đã đầy, gửi đi và tạo batch mới
                break

        self.producer.send_batch(event_data_batch)
        self.total_sent += events_in_batch
        return events_in_batch

    def run(self, interval: float = SALES_GENERATION_INTERVAL):
        """
        Chạy vòng lặp chính để liên tục sinh và gửi dữ liệu.
        
        Args:
            interval: Thời gian chờ giữa các batch (giây).
        """
        print("=" * 60)
        print("  HỆ THỐNG SINH DỮ LIỆU BÁN HÀNG THỜI GIAN THỰC")
        print(f"  Event Hub: {EVENT_HUB_NAME}")
        print(f"  Batch Size: {BATCH_SIZE}")
        print(f"  Interval: {interval}s")
        print("  Nhấn Ctrl+C để dừng")
        print("=" * 60)

        try:
            while self.running:
                count = self.send_batch()
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(
                    f"[{timestamp}] Đã gửi {count} sự kiện | "
                    f"Tổng: {self.total_sent}"
                )
                time.sleep(interval)
        except Exception as e:
            print(f"[ERROR] Lỗi: {e}")
        finally:
            self.producer.close()
            print(f"[INFO] Đã đóng kết nối. Tổng sự kiện đã gửi: {self.total_sent}")


def main():
    generator = SalesDataGenerator()
    generator.run()


if __name__ == "__main__":
    main()
