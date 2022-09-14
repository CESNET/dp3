# syntax=docker/dockerfile:1

# Base interpreter with installed requirements
FROM python:3.9-slim as base
RUN apt-get update; apt-get install -y git libpq-dev gcc
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . .