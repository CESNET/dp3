#!/usr/bin/env python3
"""Simple datapoint sender script for testing local DP3 instance."""
import gzip
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
        parser.error(f"The file {arg} does not exist!")
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
    now = datetime.utcnow()
    shift = now - dp["t1"]
    dp["t1"] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

    if pd.isna(shift):
        del dp["t2"]
        return dp
    dp["t2"] = (dp["t2"] + shift).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
    if not isinstance(dp["v"], dict):
        return dp

    ts_fields = ["time_first", "time_last", "time"]
    for field in ts_fields:
        if field in dp["v"]:
            shifted = []
            for item in dp["v"][field]:
                shifted.append(
                    (datetime.fromisoformat(item) + shift).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
                )
            dp["v"][field] = shifted
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
    for _i, dp in datapoints.iterrows():
        yield dp_factory(dp)


def worker_thread(i: int, args, stop_event: Event, dp_q: Queue, q_empty: Event, queue: Queue):
    log.debug("Starting")

    while not stop_event.is_set():
        if dp_q.empty():
            q_empty.set()
        dps = dp_q.get()
        if dp_q.empty():
            q_empty.set()

        if dps is None:
            log.debug("Received stop event, exiting.")
            break
        send_dps(i, args, stop_event, dps, queue)


def send_dps(i: int, args, stop_event: Event, datapoints, queue: Queue):
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
        t_sent = time.time()
        try:
            response = requests.post(url=datapoints_url, json=batch)
            t_received = time.time()
            if response.status_code == requests.codes.ok:
                attributes = {dp["attr"] for dp in batch}
                log.debug(
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
                "(%.3fs) %s - %s datapoints of attribute(s) %s",
                t_error - t_sent,
                err,
                len(batch),
                ", ".join(attributes),
            )
            stop_event.set()
            time.sleep(0.5)  # Make sure the reader does not clear the empty queue signal at startup
            queue_empty.set()
            break
        except requests.exceptions.InvalidJSONError as err:
            log.exception(err)
            log.error(batch)
        except KeyboardInterrupt:
            log.info("Received user interrupt, exiting.")
            break

    log.info("Sent %s datapoints in %.3fs", dps_sent, request_time)
    queue.put((i, dps_sent, request_time))


def reader_thread(
    file_path: str,
    dp_q: Queue,
    q_empty: Event,
    stop_running: Event,
    buffer_size: int = 1000,
    prefetch_size: int = 4,
):
    startup = True
    try:
        dp_iter = datapoint_reader(file_path, buffer_size)
        while not stop_running.is_set():
            for _i in range(prefetch_size):
                dp_chunk = next(dp_iter)
                log.info("Read %s datapoints (%s)", dp_chunk.shape[0], _i + 1)
                dp_q.put(dp_chunk)
            if startup:
                startup = False
                q_empty.clear()

            q_empty.wait()
            q_empty.clear()

    except KeyboardInterrupt:
        log.info("Received user interrupt, exiting.")
    except Exception as e:
        log.exception(e)

    for _ in range(prefetch_size):
        dp_q.put(None)


def datapoint_reader(file_path: str, buffer_size: int = 10000):
    def yield_transform(buf: list[bytes]):
        return pd.read_json(
            b"".join(buf).decode("utf-8"),
            orient="records",
            convert_dates=["t1", "t2"],
            lines="jsonl" in args.input_file,
        ).fillna(value={"c": 1.0})

    open_fn = open if not file_path.endswith(".gz") else gzip.open

    with open_fn(file_path, "rb") as file:
        buffer = []
        for line in file:
            buffer.append(line)
            if len(buffer) >= buffer_size:
                yield yield_transform(buffer)
                buffer = []
        if buffer:
            yield yield_transform(buffer)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Simple datapoint sender script for testing local DP3 instance."
    )
    parser.add_argument(
        "input_file",
        help="DataPoint log file in JSON or JSONL format (pandas orient=records)",
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

    prev = 0
    dp_queue = Queue()
    telem_queue = Queue()

    # Send them
    queue_empty = Event()
    stop = Event()
    workers = [
        Thread(
            target=worker_thread,
            name=f"Worker-{i}",
            args=(i, args, stop, dp_queue, queue_empty, telem_queue),
        )
        for i in range(args.workers)
    ]
    reader = Thread(
        target=reader_thread,
        name="Reader",
        args=(args.input_file, dp_queue, queue_empty, stop),
        kwargs={"prefetch_size": max(4, args.workers * 2)},
    )
    try:
        reader.start()
        log.info("Starting %s workers", len(workers))
        [w.start() for w in workers]
        log.debug("Avaiting result")
        [w.join() for w in workers]
    except KeyboardInterrupt:
        log.info("Received user interrupt, exiting.")
        stop.set()
        queue_empty.set()
        reader.join()
        [w.join() for w in workers]

    dps_sent, request_time = 0, 0

    w_times = {i: 0 for i in range(args.workers)}
    while not telem_queue.empty():
        i, dps_sent_, request_time_ = telem_queue.get()
        dps_sent += dps_sent_
        w_times[i] += request_time_
    request_time = max(w_times.values())

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
