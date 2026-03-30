import tushare as ts
import json
import time
from kafka import KafkaProducer

# Tushare初始化
ts.set_token('2a9178eb3f3a9d1911e4f9a3b1a08b9731b72a7aac0b8c95f28b699e')
pro = ts.pro_api()

# Kafka生产者
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_realtime_data(symbol):
    # 获取实时数据（示例用日线模拟实时）
    df = pro.daily(ts_code=symbol, start_date="20220101")
    for _, row in df.iterrows():
        data = {
            "symbol": symbol,
            "trade_date": row['trade_date'],
            "close": row['close'],
            "high": row['high'],
            "low": row['low'],
            "vol": row['vol']
        }
        producer.send('stock_raw', value=data)
        print(f"Sent: {data}")
        time.sleep(1)  # 模拟实时流

if __name__ == "__main__":
    fetch_realtime_data("600519.SH")  # 茅台股票