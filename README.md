# TweeeeDBT

- setting up zookeeper and kafka
```bash
docker compose up
```

```bash
docker ps # should show zookeeper and kafka container up and running
```

- python stuff
```bash
pip install -r requirements.txt
python3 producer.py
python3 consumer.py
```

- to test kafka
```bash
docker exec -it <kafka-image-name> bash
```

- inside the container run 
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```