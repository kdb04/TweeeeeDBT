# TweeeeDBT

## Zookeeper and Kafka setup
```bash
docker compose up
```

```bash
docker ps # should show zookeeper and kafka container up and running
```

- to test kafka
```bash
docker exec -it tweeeeedbt-kafka-1 bash
```

- inside the container run 
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Spark setup
- Requirements 
`java 17 python 3.8.10` <br>
ps: if you have higher version of either things pyspark might probably not get installed corrected and even if it does it will not work correctly :) 
```bash
wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
```

```bash
tar -xvzf spark-3.5.5-bin-hadoop3.tgz
mv spark-3.5.5-bin-hadoop3 ~/spark
```

```bash
nano ~/.bashrc # or zshrc
```

```bash
export SPARK_HOME=~/spark
export PATH=$SPARK_HOME/bin:$PATH
```

## How to run

- Make sure that each time the old data is flushed
```bash
bash reset-kafka.sh
```

- Run each of the following commands in seperate terminals and in same order
```bash
python3 producer.py
```

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 stream_processing.py
```

```bash
python3 consumer_userverify.py
```
