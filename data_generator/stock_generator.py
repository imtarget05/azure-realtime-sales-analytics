"""
Sinh dữ liệu chứng khoán giả lập gửi đến Azure Event Hubs.
Mô phỏng dữ liệu cổ phiếu biến động theo thời gian thực.
"""

import json
import random
import time
import signal
import sys
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

sys.path.insert(0, ".")
from config.settings import EVENT_HUB_CONNECTION_STRING

STOCK_EVENT_HUB_NAME = "stock-events"


class StockDataGenerator:
    """Sinh dữ liệu chứng khoán giả lập."""

    STOCKS = [
        {"symbol": "MSFT", "name": "Microsoft Corp", "sector": "Technology", "base_price": 420.0},
        {"symbol": "AAPL", "name": "Apple Inc", "sector": "Technology", "base_price": 195.0},
        {"symbol": "GOOGL", "name": "Alphabet Inc", "sector": "Technology", "base_price": 175.0},
        {"symbol": "AMZN", "name": "Amazon.com Inc", "sector": "Consumer", "base_price": 185.0},
        {"symbol": "TSLA", "name": "Tesla Inc", "sector": "Automotive", "base_price": 250.0},
        {"symbol": "NVDA", "name": "NVIDIA Corp", "sector": "Technology", "base_price": 875.0},
        {"symbol": "META", "name": "Meta Platforms", "sector": "Technology", "base_price": 500.0},
        {"symbol": "JPM", "name": "JPMorgan Chase", "sector": "Finance", "base_price": 195.0},
        {"symbol": "V", "name": "Visa Inc", "sector": "Finance", "base_price": 280.0},
        {"symbol": "WMT", "name": "Walmart Inc", "sector": "Consumer", "base_price": 165.0},
    ]

    def __init__(self):
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STRING,
            eventhub_name=STOCK_EVENT_HUB_NAME,
        )
        self.running = True
        self.total_sent = 0
        # Theo dõi giá hiện tại của mỗi cổ phiếu
        self.current_prices = {s["symbol"]: s["base_price"] for s in self.STOCKS}
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        print(f"\n[INFO] Đang dừng... Tổng cộng {self.total_sent} bản ghi chứng khoán.")
        self.running = False

    def generate_stock_event(self, stock: dict) -> dict:
        """Tạo dữ liệu biến động giá cổ phiếu."""
        now = datetime.now(timezone.utc)
        symbol = stock["symbol"]

        # Random walk cho giá cổ phiếu
        change_pct = random.gauss(0, 0.002)  # thay đổi ~0.2% mỗi tick
        old_price = self.current_prices[symbol]
        new_price = round(old_price * (1 + change_pct), 2)
        self.current_prices[symbol] = new_price

        price_change = round(new_price - old_price, 2)
        change_percent = round(change_pct * 100, 4)

        volume = random.randint(100, 50000)

        # Tính các mức giá High/Low trong tick
        high = round(max(old_price, new_price) * (1 + random.uniform(0, 0.001)), 2)
        low = round(min(old_price, new_price) * (1 - random.uniform(0, 0.001)), 2)

        return {
            "timestamp": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "hour": now.hour,
            "minute": now.minute,
            "symbol": symbol,
            "company_name": stock["name"],
            "sector": stock["sector"],
            "open_price": old_price,
            "close_price": new_price,
            "high_price": high,
            "low_price": low,
            "price_change": price_change,
            "change_percent": change_percent,
            "volume": volume,
            "market_cap_millions": round(new_price * random.randint(500, 3000), 2),
        }

    def send_stock_data(self):
        """Gửi dữ liệu chứng khoán cho tất cả cổ phiếu."""
        event_data_batch = self.producer.create_batch()
        count = 0

        for stock in self.STOCKS:
            event = self.generate_stock_event(stock)
            event_json = json.dumps(event)
            try:
                event_data_batch.add(EventData(event_json))
                count += 1
            except ValueError:
                break

        self.producer.send_batch(event_data_batch)
        self.total_sent += count
        return count

    def run(self, interval: float = 5.0):
        """Chạy vòng lặp sinh dữ liệu chứng khoán (mỗi 5 giây)."""
        print("=" * 60)
        print("  HỆ THỐNG SINH DỮ LIỆU CHỨNG KHOÁN THỜI GIAN THỰC")
        print(f"  Event Hub: {STOCK_EVENT_HUB_NAME}")
        print(f"  Stocks: {', '.join(s['symbol'] for s in self.STOCKS)}")
        print(f"  Interval: {interval}s")
        print("  Nhấn Ctrl+C để dừng")
        print("=" * 60)

        try:
            while self.running:
                count = self.send_stock_data()
                timestamp = datetime.now().strftime("%H:%M:%S")
                # Hiển thị giá hiện tại
                prices_str = " | ".join(
                    f"{s}: ${self.current_prices[s]:.2f}"
                    for s in list(self.current_prices.keys())[:5]
                )
                print(f"[{timestamp}] Gửi {count} | Tổng: {self.total_sent} | {prices_str}")
                time.sleep(interval)
        except Exception as e:
            print(f"[ERROR] Lỗi: {e}")
        finally:
            self.producer.close()
            print(f"[INFO] Đã đóng. Tổng bản ghi: {self.total_sent}")


def main():
    generator = StockDataGenerator()
    generator.run()


if __name__ == "__main__":
    main()
