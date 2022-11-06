FROM python:3.11-slim

WORKDIR /opt

RUN apt-get update && apt-get install -y curl git

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
