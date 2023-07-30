# syntax=docker/dockerfile:1

# Base interpreter with installed requirements
FROM python:3.9-slim as base
RUN apt-get update; apt-get install -y  git

# Install requirements
WORKDIR /{{DP3_APP}}/
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip; \
    pip install -r requirements.txt

# When the application changes, only the COPY . /{{DP3_APP}}/ line will be re-run
# (this means changes in modules, config files, etc. will not trigger a re-install of requirements)
COPY . /{{DP3_APP}}/