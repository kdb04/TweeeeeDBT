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

## DB setup (might not be exact on your system)

- installation

```bash
sudo apt install postgresql postgresql-contrib
```

- enabling the psl service

```bash
sudo systemctl status postgresql
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

- access by switching to default postgres user
```bash
sudo -i -u postgres
```
```bash
psql
```

- creating user and granting permission inside the psql shell
```bash
-- Create a database
CREATE DATABASE tweedbt;

-- Create a user with a password
CREATE USER <username> WITH PASSWORD '<password>'; -- these values should go into your .env

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE mydb TO <username>;
```
- applying the schema
```bash
cd DB/
psql -U <username> -d tweedbt -f schema.sql
```

- accessing psql
```bash
psql -U <username> -d tweedbt
```

>    - \l to list all dbs
>    - \d to list all relations

## How to run Pyspark, Kafka integration 

- Make sure that each time the old data is flushed
```bash
bash reset-kafka.sh
```

- Run each of the following commands in seperate terminals and in same order

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 stream_processing.py
```

```bash
python3 consumer_<whatever>.py
```

```bash
python3 producer.py
```

## ToDo 
- [ ] Folder Structuring
- [ ] System Architecture Diagram