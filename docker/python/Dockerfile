# syntax=docker/dockerfile:1

# Base interpreter with installed requirements
FROM python:3.9-slim as base
RUN apt-get update; apt-get install -y  \
    gcc \
    git

WORKDIR /dp3/
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip; \
    pip install -r requirements.txt

COPY . /dp3/
RUN pip install -e /dp3
