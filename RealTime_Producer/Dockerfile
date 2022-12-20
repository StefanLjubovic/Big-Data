FROM python:3.7-slim

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install --proxy http://192.168.18.1:8080 --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="localhost:9093" 
ENV MOVIE_CRITICS = "movie-critics"
ENV ROTTEN_MOVIES = "rotten-movies"

# ENV HTTP_PROXY=http://192.168.18.1:8080
# ENV HTTPS_PROXY=http://192.168.18.1:8080

CMD ["python", "-u", "./producer.py"]