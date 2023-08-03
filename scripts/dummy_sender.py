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


def cherry_pick_send(datapoints, dp_factory, log, args):
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

    log.info("Selected %s datapoints", len(dp_list))
    return dp_list


def send_all(datapoints, dp_factory, *_):
    for _, dp in datapoints.iterrows():
        yield dp_factory(dp)


if __name__ == "__main__":
    parser = ArgumentParser("Simple datapoint sender script for testing local DP3 instance.")
    parser.add_argument(
        "input_file",
        help="DataPoint log file in JSON format (pandas orient=records)",
        type=lambda x: get_valid_path(parser, x),
    )
    parser.add_argument(
        "mode",
        help="Mode of operation for the sender. "
        "Either cherry-pick datapoints by attribute, or send the entire log.",
        choices=["cherry-pick", "all"],
    )
    parser.add_argument(
        "--endpoint-url",
        help="base DP3 endpoint, default='http://127.0.0.1:5000'",
        default="http://127.0.0.1:5000",
        type=str,
        dest="endpoint_url",
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
        "--shift-time",
        help="shift timestamps so that 't1' is the current UTC time",
        default=False,
        action="store_true",
        dest="shift_time",
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
    ).fillna(value={"c": 1.0})
    log.info("Input file contains %s DataPoints", datapoints.shape[0])

    if args.mode == "cherry-pick":
        dps = cherry_pick_send(datapoints, dp_factory, log, args)
    elif args.mode == "all":
        dps = send_all(datapoints, dp_factory, log, args)
    else:
        raise ValueError(f"Invalid mode selected: {args.mode}")

    # Send them
    datapoints_url = f"{args.endpoint_url}/datapoints"
    for batch in batched(dps, args.chunk):
        payload = json.dumps(batch)
        log.debug(payload)
        try:
            response = requests.post(url=datapoints_url, json=batch)
            if response.status_code == requests.codes.ok:
                attributes = {dp["attr"] for dp in batch}
                log.info("%s datapoints of attribute(s) %s OK", len(batch), ", ".join(attributes))
            else:
                log.error("Payload: %s", payload)
                log.error(
                    "Request failed: %s: %s", response.reason, response.content.decode("utf-8")
                )
        except requests.exceptions.ConnectionError as err:
            log.exception(err)
        except requests.exceptions.InvalidJSONError as err:
            log.exception(err)
            log.error(batch)
