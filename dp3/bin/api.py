#!/usr/bin/env python3
"""
Run the DP3 API using uvicorn.
"""
import uvicorn

from dp3.api.main import app


def run():
    uvicorn.run(app, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    run()
