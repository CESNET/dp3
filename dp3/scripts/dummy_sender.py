"""Simple datapoint sender script for testing local DP3 instance."""

import json
import logging
import os
import time
from argparse import ArgumentParser
from datetime import datetime
from itertools import islice
from queue import Queue
from threading import Event, Thread

import pandas as pd
import requests


def get_valid_path(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return os.path.abspath(arg)


def get_datapoint_from_row(row):
    dp = dict(row.items())
    if pd.isna(dp["t1"]):
        del dp["t1"]
        del dp["t2"]
        return dp
    dp["t1"] = dp["t1"].strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

    if pd.isna(dp["t2"]):
        del dp["t2"]
        return dp
    dp["t2"] = dp["t2"].strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
    return dp


def get_shifted_datapoint_from_row(row):
    dp = dict(row.items())
    if pd.isna(dp["t1"]):
        del dp["t1"]
        del dp["t2"]
        return dp
    duration = dp["t2"] - dp["t1"]
    dp["t1"] = (datetime.utcnow()).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

    if pd.isna(duration):
        del dp["t2"]
        return dp
    dp["t2"] = (datetime.utcnow() + duration).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
    return dp


def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
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


def send_dps(args, stop_event: Event, datapoints, queue: Queue):
    log.debug("Starting")

    if args.mode == "cherry-pick":
        dps = cherry_pick_send(datapoints, dp_factory, log, args)
    elif args.mode == "all":
        dps = send_all(datapoints, dp_factory, log, args)
    else:
        raise ValueError(f"Invalid mode selected: {args.mode}")

    request_time = 0
    dps_sent = 0

    datapoints_url = f"{args.endpoint_url}/datapoints"
    for batch in batched(dps, args.chunk):
        if stop_event.is_set():
            log.info("Received stop event, exiting.")
            break
        payload = json.dumps(batch)
        log.debug(payload)
        try:
            t_sent = time.time()
            response = requests.post(url=datapoints_url, json=batch)
            t_received = time.time()
            if response.status_code == requests.codes.ok:
                attributes = {dp["attr"] for dp in batch}
                log.info(
                    "(%.3fs) %s datapoints of attribute(s) %s OK",
                    t_received - t_sent,
                    len(batch),
                    ", ".join(attributes),
                )
            else:
                log.error("Payload: %s", payload)
                log.error(
                    "Request failed: %s: %s", response.reason, response.content.decode("utf-8")
                )

            dps_sent += len(batch)
            request_time += t_received - t_sent

        except requests.exceptions.ConnectionError as err:
            t_error = time.time()
            attributes = {dp["attr"] for dp in batch}
            log.error(
                "%(%.3f s) %s - %s datapoints of attribute(s) %s",
                t_error - t_sent,
                err,
                len(batch),
                ", ".join(attributes),
            )
        except requests.exceptions.InvalidJSONError as err:
            log.exception(err)
            log.error(batch)
        except KeyboardInterrupt:
            log.info("Received user interrupt, exiting.")
            break

    log.info("Sent %s datapoints in %.3fs", dps_sent, request_time)
    queue.put((dps_sent, request_time))


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
    parser.add_argument(
        "--workers",
        "-w",
        help="number of workers to use, default=1",
        default=1,
        type=int,
    )
    args = parser.parse_args()
    if args.chunk is None:
        args.chunk = args.count
    dp_factory = get_shifted_datapoint_from_row if args.shift_time else get_datapoint_from_row
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)-15s,%(threadName)s,[%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    log = logging.getLogger("DataPointSender")

    datapoints = pd.read_json(
        args.input_file,
        orient="records",
        convert_dates=["t1", "t2"],
    ).fillna(value={"c": 1.0})
    log.info("Input file contains %s DataPoints", datapoints.shape[0])

    prev = 0
    step = datapoints.shape[0] // args.workers
    chunked = (datapoints.iloc[i : i + step, :] for i in range(0, datapoints.shape[0], step))
    queue = Queue()

    # Send them
    stop = Event()
    workers = [
        Thread(
            target=send_dps,
            args=(args, stop, chunk, queue),
        )
        for i, chunk in enumerate(chunked)
        if i < args.workers
    ]
    try:
        if args.workers == 1:
            send_dps(args, stop, datapoints, queue)
        else:
            log.info("Starting %s workers", len(workers))
            [w.start() for w in workers]
            log.debug("Avaiting result")
            [w.join() for w in workers]
    except KeyboardInterrupt:
        log.info("Received user interrupt, exiting.")
        stop.set()
        if args.workers > 1:
            [w.join() for w in workers]

    dps_sent, request_time = 0, 0

    while not queue.empty():
        dps_sent_, request_time_ = queue.get()
        dps_sent += dps_sent_
        request_time = max(request_time, request_time_)

    if dps_sent != 0:
        log.info(
            "Total: Sent %s datapoints in %.3fs (throughput: %.3f DPs/s; %.4f s/DP)",
            dps_sent,
            request_time,
            dps_sent / request_time,
            request_time / dps_sent,
        )
    else:
        log.info("No datapoints were sent.")
