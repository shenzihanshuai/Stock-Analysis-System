from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# 定义Schema
schema = StructType([
    StructField("symbol", StringType()),
    StructField("trade_date", StringType()),
    StructField("close", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("vol", DoubleType())
])

spark = SparkSession.builder \
    .appName("StockRealtimeAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# 从Kafka读取数据
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_raw") \
    .load()

# 解析JSON数据
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 计算技术指标
def calculate_technical_indicators(df):
    window_5 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-4, 0)
    window_10 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-9, 0)
    window_9 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-8, 0)
    window_12 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-11, 0)
    window_26 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-25, 0)

    df = df.withColumn("MA5", avg("close").over(window_5))
    df = df.withColumn("MA10", avg("close").over(window_10))

    exp12 = avg("close").over(window_12)
    exp26 = avg("close").over(window_26)
    df = df.withColumn("DIF", exp12 - exp26)
    df = df.withColumn("DEA", avg("DIF").over(window_9))
    df = df.withColumn("MACD", 2 * (col("DIF") - col("DEA")))

    min_low = min("low").over(window_9)
    max_high = max("high").over(window_9)
    df = df.withColumn("RSV", 100 * ((col("close") - min_low) / (max_high - min_low)))

    # K、D 用 RSV 近似
    df = df.withColumn("K", col("RSV"))
    df = df.withColumn("D", col("RSV"))
    return df

result_df = calculate_technical_indicators(parsed_df)

def process_batch(df, epoch_id):
    import pandas as pd
    if df.rdd.isEmpty():
        return
    pdf = df.toPandas()
    if pdf.empty:
        return
    pdf = pdf.sort_values(['symbol', 'trade_date'])
    pdf['MA5'] = pdf.groupby('symbol')['close'].transform(lambda x: x.rolling(5, min_periods=1).mean())
    pdf['MA10'] = pdf.groupby('symbol')['close'].transform(lambda x: x.rolling(10, min_periods=1).mean())
    pdf['EMA12'] = pdf.groupby('symbol')['close'].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    pdf['EMA26'] = pdf.groupby('symbol')['close'].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    pdf['DIF'] = pdf['EMA12'] - pdf['EMA26']
    pdf['DEA'] = pdf.groupby('symbol')['DIF'].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    pdf['MACD'] = 2 * (pdf['DIF'] - pdf['DEA'])
    pdf['min_low9'] = pdf.groupby('symbol')['low'].rolling(9, min_periods=1).min().reset_index(0, drop=True)
    pdf['max_high9'] = pdf.groupby('symbol')['high'].rolling(9, min_periods=1).max().reset_index(0, drop=True)
    pdf['RSV'] = 100 * (pdf['close'] - pdf['min_low9']) / (pdf['max_high9'] - pdf['min_low9'])
    pdf['K'] = pdf.groupby('symbol')['RSV'].apply(lambda x: x.ewm(alpha=1/3, adjust=False).mean()).reset_index(level=0, drop=True)
    pdf['D'] = pdf.groupby('symbol')['K'].apply(lambda x: x.ewm(alpha=1/3, adjust=False).mean()).reset_index(level=0, drop=True)
    pdf = pdf.where(pd.notnull(pdf), None)
    spark_df = spark.createDataFrame(pdf)
    spark_df.select(to_json(struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "stock_indicators") \
        .save()
    print(pdf.groupby('symbol').size())
    print(pdf.groupby('symbol')['close'].apply(list))
    print(pdf[['symbol', 'trade_date', 'close', 'EMA12', 'EMA26', 'DIF', 'K', 'D']].tail(20))

query = parsed_df.writeStream.foreachBatch(process_batch).trigger(processingTime='10 seconds').start()
query.awaitTermination()

print("Streaming query started")
query.awaitTermination()
print("Streaming query terminated")  # 如果看到这行，说明流处理异常停止了