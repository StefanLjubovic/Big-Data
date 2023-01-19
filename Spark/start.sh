#!/bin/bash

sudo docker exec -it spark-master ./spark/bin/spark-submit  ../spark/primeri/app.py &
sudo docker exec -it spark-master ./spark/bin/spark-submit  --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/transformation_zone.py &
sudo docker exec -it spark-master ./spark/bin/spark-submit  --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/batch.py &
sudo docker exec -it ./spark/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/consumer.py &
sudo docker exec -it ./spark/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/consumer_join.py &
sudo docker exec -it ./spark/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/consumer_window.py