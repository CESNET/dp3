# syntax=docker/dockerfile:1

# Base interpreter with installed requirements
FROM python:3.9-slim as base
RUN apt-get update; apt-get install -y git libpq-dev gcc
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . .

FROM base as unittest
CMD [ "python", "-m", "unittest",  "tests/test_common/test_config.py" ]

FROM base as apitest
CMD [ "python", "api/tests/test_api.py",  "http://receiver_api:5000/", "2" ]

FROM base as apitest2
CMD [ "python", "-m", "unittest",  "api/tests/test_probability.py" ]
