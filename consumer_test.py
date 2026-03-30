from kafka import KafkaConsumer
import json
import pandas as pd

consumer = KafkaConsumer(
    'stock_indicators',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',  # 只看新数据
    enable_auto_commit=True
)

print("开始监听 stock_indicators topic ...")
for msg in consumer:
    print(msg.value)

pdf = pdf.where(pd.notnull(pdf), None)