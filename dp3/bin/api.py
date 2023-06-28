#!/usr/bin/env python3
"""
Run the DP3 API using uvicorn.
"""
import argparse

import uvicorn


def run():
    parser = argparse.ArgumentParser(description="Run the DP3 API using uvicorn.")
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="The host to bind to. (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port", type=int, default=5000, help="The port to bind to. (default: 5000)"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="Enable auto-reload of the api. (API changes will be picked up automatically.)",
    )
    args = parser.parse_args()

    uvicorn.run("dp3.api.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    run()
