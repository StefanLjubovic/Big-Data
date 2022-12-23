FROM python:3.7-slim

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install --proxy http://192.168.18.1:8080 --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="localhost:9092" 
ENV MOVIE_DETAILS = "movie-recensions"

# ENV HTTP_PROXY=http://192.168.18.1:8080
# ENV HTTPS_PROXY=http://192.168.18.1:8080

CMD ["python", "-u", "./producer.py"]