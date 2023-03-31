"""Simple datapoint sender script for testing local DP3 instance."""

import json
import logging
import os
from argparse import ArgumentParser
from datetime import datetime
from itertools import islice

import pandas as pd
import requests


def get_valid_path(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return os.path.abspath(arg)


def get_datapoint_from_row(row):
    dp = dict(row.items())
    dp["t1"] = dp["t1"].strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
    dp["t2"] = dp["t2"].strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
    return dp


def get_shifted_datapoint_from_row(row):
    dp = dict(row.items())
    duration = dp["t2"] - dp["t1"]
    dp["t1"] = (datetime.utcnow()).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
    dp["t2"] = (datetime.utcnow() + duration).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
    return dp


def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


if __name__ == "__main__":
    parser = ArgumentParser("Simple datapoint sender script for testing local DP3 instance.")
    parser.add_argument(
        "input_file",
        help="DataPoint log file in JSON format (pandas orient=records)",
        type=lambda x: get_valid_path(parser, x),
    )
    parser.add_argument(
        "--endpoint_url",
        help="base DP3 endpoint, default='http://127.0.0.1:5000'",
        default="http://127.0.0.1:5000",
        type=str,
    )
    parser.add_argument(
        "-n",
        dest="count",
        help="number of DataPoints to send per attribute, default=50",
        default=50,
        type=int,
    )
    parser.add_argument(
        "-c",
        "--chunk",
        dest="chunk",
        help="number of DataPoints to send per request, default=same as count",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--shift_time",
        help="shift timestamps so that 't1' is the current UTC time",
        default=False,
        action="store_true",
    )
    args = parser.parse_args()
    if args.chunk is None:
        args.chunk = args.count
    dp_factory = get_shifted_datapoint_from_row if args.shift_time else get_datapoint_from_row
    logging.basicConfig(level=logging.INFO, format="%(name)s [%(levelname)s] %(message)s")
    log = logging.getLogger("DataPointSender")

    datapoints = pd.read_json(
        args.input_file,
        orient="records",
        convert_dates=["t1", "t2"],
    )
    log.info("Input file contains %s DataPoints", datapoints.shape[0])

    # Get all datapoints that will be sent.
    attributes = datapoints["attr"].unique()
    log.debug("Found %s attributes", len(datapoints))
    log.debug(attributes)
    dp_list = []
    for attr in attributes:
        selected = datapoints[datapoints["attr"] == attr]
        selected_dps = selected.iloc[: args.count, :].apply(dp_factory, axis=1).to_list()
        log.debug("Selected %s datapoints of attribute %s", len(selected_dps), attr)
        dp_list.extend(selected_dps)

    # Send them
    log.info("Sending %s datapoints", len(dp_list))
    datapoints_url = f"{args.endpoint_url}/datapoints"
    for batch in batched(dp_list, args.chunk):
        payload = json.dumps(batch)
        log.debug(payload)
        try:
            response = requests.post(url=datapoints_url, json=batch)
        except requests.exceptions.ConnectionError as err:
            print(err)

        if response.status_code == 200:
            attributes = {dp["attr"] for dp in batch}
            log.info("%s datapoints of attribute(s) %s OK", len(batch), ", ".join(attributes))
        else:
            log.error("Payload: %s", payload)
            log.error("Request failed: %s: %s", response.reason, response.content.decode("utf-8"))
