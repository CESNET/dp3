# syntax=docker/dockerfile:1

FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_SYSTEM_PYTHON=1

RUN pip install --no-cache-dir "uv==0.9.7"

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /{{DP3_APP}}

COPY requirements.txt ./
RUN uv pip install --no-cache -r requirements.txt

COPY . .
