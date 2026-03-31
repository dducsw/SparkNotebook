setup:
	python -m venv venv
	.\venv\Scripts\activate && pip install -r requirements.txt

install:
	pip install -r requirements.txt

clean:
	rmdir /s /q venv

# Docker commands
up:
	docker compose up -d

down:
	docker compose down

# Kafka commands (sử dụng docker compose exec vào broker-1)
kafka-list:
	docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-create:
	docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $(topic) --partitions 3 --replication-factor 3

kafka-producer:
	docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $(topic)

kafka-consumer:
	docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $(topic) --from-beginning

kafka-status:
	docker compose exec broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
