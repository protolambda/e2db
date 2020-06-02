FROM python:3.8

COPY docker_requirements.txt /

RUN pip install -r /docker_requirements.txt

COPY . /app

WORKDIR /app
