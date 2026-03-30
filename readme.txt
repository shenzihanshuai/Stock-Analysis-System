## 项目文档
- [查看实验报告](./reports/大数据股票分析报告.pdf)
- [查看数据分析说明](./reports/analysis_notes.md)

版本控制
jdk1.8 hadoop 2.7.7 kafka2.10-0.10.00 spark2.4.5 python3.6 flask gevent

1.启动zookeeper和Kafka
cd /home/shenzihan/bigdata/kafka_2.10-0.10.0.0
bin/zookeeper-server-start.sh config/zookeeper.properties
cd /home/shenzihan/bigdata/kafka_2.10-0.10.0.0
bin/kafka-server-start.sh config/server.properties
2.创建kafka topic
cd /home/shenzihan/bigdata/kafka_2.10-0.10.0.0
bin/kafka-topics.sh --create \
--zookeeper localhost:2181 \
--topic stock_raw \
--partitions 1 \
--replication-factor 1

bin/kafka-topics.sh --create \
--zookeeper localhost:2181 \
--topic stock_indicators \
--partitions 1 \
--replication-factor 1

//查看topic
bin/kafka-topics.sh --list --zookeeper localhost:2181

3.启动组件
cd /home/shenzihan/bigdata/finalwork/code
python producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 spark_streaming.py
python app.py

