FROM python:3.7-slim

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install --proxy http://192.168.18.1:8080 --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="kafka2:19093" 
ENV MOVIE_CRITICS = "rotten-recensions"
ENV CORE_CONF_fs_defaultFS = "hdfs://namenode:9000"

# ENV HTTP_PROXY=http://192.168.18.1:8080
# ENV HTTPS_PROXY=http://192.168.18.1:8080

CMD ["python", "-u", "./rt_producer.py"]