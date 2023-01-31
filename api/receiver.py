#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

import requests
from flask import Flask, jsonify, request

from dp3.common.attrspec import AttrType
from dp3.common.config import load_attr_spec, read_config_dir
from dp3.common.task import Task
from dp3.common.utils import parse_rfc_time
from dp3.database.database import EntityDatabase
from dp3.task_processing.task_queue import TaskQueueWriter

app = Flask(__name__)

# Get application name and directory containing config files.
# Can be specified as command-line parameters (when run as stand-alone testing server)
# or as environment variables.
if __name__ == "__main__" and len(sys.argv) > 1:
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument("app_name", help="Identification of DP3 application")
    argparser.add_argument("conf_dir", help="Configuration directory")
    argparser.add_argument(
        "dp_log_file", nargs="?", help="File to store all incoming datapoints (as CSV)"
    )
    args = argparser.parse_args()
    app_name = args.app_name
    conf_dir = args.conf_dir
    dp_log_file = args.dp_log_file
elif "DP3_CONFIG_DIR" in os.environ and "DP3_APP_NAME" in os.environ:
    conf_dir = os.environ["DP3_CONFIG_DIR"]
    app_name = os.environ["DP3_APP_NAME"]
    dp_log_file = os.environ["DP3_DP_LOG_FILE"]
else:
    print(
        "Error: DP3_APP_NAME and DP3_CONFIG_DIR environment variables must be set.", file=sys.stderr
    )
    print(
        "  DP3_APP_NAME - application name used to distinguish this app from other dp3-based apps",
        file=sys.stderr,
    )
    print("  DP3_CONFIG_DIR - directory containing configuration files", file=sys.stderr)
    print(
        "  DP3_DP_LOG_FILE - (optional) file to store all incoming datapoints (as CSV)",
        file=sys.stderr,
    )
    sys.exit(1)

# Dictionary containing platform configuration
config = None

# Dictionary containing entity / attribute specification
attr_spec = {}

# TaskQueueWriter instance used for sending tasks to the processing core
task_writer = None

# Database handler
db = None

# verbose (debug) mode
verbose = False

# Logging initialization
log_format = "%(asctime)-15s [%(levelname)s] %(message)s"
log_dateformat = "%Y-%m-%dT%H:%M:%S"
logging.basicConfig(level=logging.WARNING, format=log_format, datefmt=log_dateformat)
log = logging.getLogger()
log.setLevel(logging.DEBUG if verbose else logging.INFO)

# Load configuration and connect to dp3 message broker.
# This needs to be called before any request is made.
# Load configuration and entity/attribute specification
try:
    config = read_config_dir(conf_dir, recursive=True)
    attr_spec = load_attr_spec(config.get("db_entities"))
    log.debug(f"Loaded configuration: {config}")
except Exception as e:
    log.exception(f"Error when reading configuration: {e}")
    sys.exit(1)

# Initialize task queue connection
try:
    task_writer = TaskQueueWriter(
        app_name,
        config.get("processing_core.worker_processes"),
        config.get("processing_core.msg_broker"),
    )
except Exception as e:
    log.exception(f"Error when connecting to task queue: {e}")
    sys.exit(1)

# Initialize database connection
try:
    db = EntityDatabase(config.get("database"), attr_spec)
except Exception as e:
    log.exception(f"Error when connecting to database: {e}")
    sys.exit(1)

log.info("Initialization completed")


def error_response(msg, msg_log_suffix=""):
    """Helper to log and return error response"""
    msg_log = msg
    if msg_log_suffix:
        msg_log += "\n" + msg_log_suffix

    log.info(msg_log)
    return msg, 400


################################################################################
# Endpoints to write data (push data-points / tasks)


def push_task(task):
    """Push given task (instance of task.Task) to platform's task queue (RabbitMQ)"""
    task_writer.put_task(task, False)


def get_multiple_datapoints_json(request):
    try:
        return request.get_json(force=True)  # force = ignore mimetype
    except Exception:
        return None


def get_datapoint_object_from_record(record: dict) -> dict:
    """Returns identical record dict, but 'type' and 'id' keys are prefixed with 'e'."""
    dp_obj = {key: value for key, value in record if key in {"attr", "v", "src", "t1", "c"}}
    if "type" in record:
        dp_obj["etype"] = record["type"]
    if "id" in record:
        dp_obj["eid"] = record["id"]
    return dp_obj


@app.route("/datapoints", methods=["POST"])
def push_multiple_datapoints():
    """
    REST endpoint to push multiple data points

    Request payload must be a JSON dict containing a list of datapoints.
    Example:
        [{dp1},{dp2},{dp3},...]
    """
    log.debug(f"Received new datapoint(s) from {request.remote_addr}")

    payload = get_multiple_datapoints_json(request)

    # Request must be valid JSON (dict) and contain a list of records
    if payload is None:
        return error_response("Not a valid JSON (or empty payload)")
    elif type(payload) is not list:
        return error_response("Payload is not list")

    # Load all datapoints from POST data
    dps = []
    for i, record in enumerate(payload, 1):
        # Check it's a dict
        if type(record) is not dict:
            return error_response(f"Invalid data-point no. {i}: Not a dictionary", f"DP: {record}")

        # Convert to DataPoint class instances
        try:
            dp_obj = get_datapoint_object_from_record(record)

            dp_model = attr_spec[dp_obj["etype"]]["attribs"][dp_obj["attr"]]._dp_model
            dps.append(dp_model.parse_obj(dp_obj))
        except Exception as e:
            return error_response(str(e))

    # Group datapoints by (etype, eid)
    tasks_dps = defaultdict(list)
    for dp in dps:
        key = (dp.etype, dp.eid)
        tasks_dps[key].append(dp)

    task_list = []

    # Create Task instances
    for k in tasks_dps:
        etype, ekey = k
        try:
            task_list.append(
                Task(attr_spec=attr_spec, etype=etype, ekey=ekey, data_points=tasks_dps[k])
            )
        except Exception as e:
            return error_response(f"Failed to create a task: {type(e)}: {str(e)}")

    # Push valid tasks to platform task queue
    for task in task_list:
        try:
            print(task)
            push_task(task)
        except Exception as e:
            return error_response(f"Failed to push task: {type(e)}: {str(e)}")

    return "Success", 200


################################################################################
# Endpoints to read data

# @app.route("/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["GET"])
# def get_attr_value(entity_type, entity_id, attr_id):
#     """
#     REST endpoint to read current value for an attribute of given entity

#     It is also possible to read historic values by providing
#     a specific timestamp as a query parameter (only for observations)

#     Entity type, entity id and attribute id must be provided

#     Examples:
#         /ip/1.2.3.4/test_attr
#         /ip/1.2.3.4/test_attr?t=2000-01-01T00:00:00
#     """
#     log.debug(f"Received new GET request from {request.remote_addr}")

#     try:
#         _ = attr_spec[entity_type]["attribs"][attr_id]
#     except KeyError:
#         response = f"Error: no specification found for {entity_type}/{attr_id}"
#         log.info(response)
#         return f"{response}\n", 400  # Bad request

#     try:
#         timestamp = request.values.get("t", None)
#         observations = attr_spec[entity_type]['attribs'][attr_id].t == AttrType.OBSERVATIONS

#         if timestamp is not None and observations:
#             content = get_historic_value(
#                 db, attr_spec, entity_type, entity_id, attr_id, parse_rfc_time(timestamp)
#             )
#         else:
#             content = db.get_attrib(entity_type, entity_id, attr_id)

#         if content is None:
#             response = f"No records found for {entity_type}/{entity_id}/{attr_id}", 404
#         else:
#             response = jsonify(content), 200  # OK
#     except Exception as e:
#         response = f"Error when querying db: {e}", 500  # Internal server error

#     return response


@app.route("/<string:entity_type>/<string:entity_id>/<string:attr_id>/history", methods=["GET"])
def get_attr_history(entity_type, entity_id, attr_id):
    """
    REST endpoint to read history of an attribute (data points from given time interval)

    Entity id, attribute id are mandatory
    Timestamps t1, t2 are optional and should be contained in query parameters

    Examples:
        /ip/1.2.3.4/test_attr/history?t1=2020-01-23T12:00:00&t2=2020-01-23T14:00:00
        /ip/1.2.3.4/test_attr/history?t1=2020-01-23T12:00:00
        /ip/1.2.3.4/test_attr/history?t2=2020-01-23T14:00:00
        /ip/1.2.3.4/test_attr/history
    """
    try:
        _ = attr_spec[entity_type]["attribs"][attr_id]
    except KeyError:
        return error_response(f"Error: no attribute specification found for '{attr_id}'")

    attr_type = attr_spec[entity_type]["attribs"][attr_id].t

    t1 = request.args.get("t1", None)
    t2 = request.args.get("t2", None)

    if t1 is not None:
        try:
            t1_parsed = parse_rfc_time(t1)
        except ValueError:
            return error_response("Error: invalid timestamp format (t1)")
    else:
        t1_parsed = datetime.fromtimestamp(0)

    if t2 is not None:
        try:
            t2_parsed = parse_rfc_time(t2)
        except ValueError:
            return error_response("Error: invalid timestamp format (t2)")
    else:
        t2_parsed = datetime.now()

    if t2_parsed < t1_parsed:
        return error_response("Error: invalid time interval (t2 < t1)")

    try:
        if attr_type == AttrType.OBSERVATIONS:
            result = db.get_observation_history(
                entity_type, attr_id, entity_id, t1_parsed, t2_parsed, sort=1
            )
            return jsonify(result)
        elif attr_type == AttrType.TIMESERIES:
            result = db.get_timeseries_history(
                entity_type, attr_id, entity_id, t1_parsed, t2_parsed, sort=1
            )
            return jsonify(result)
        else:
            return error_response("Attribute type is not observations or timeseries")
    except Exception as e:
        return f"Error when querying db: {e}", 500  # Internal server error


@app.route("/", methods=["GET"])
def ping():
    """
    REST endpoint to check whether the API is running

    Returns a simple text response ("It works!")
    """
    return "It works!"


@app.route("/workers_alive", methods=["GET"])
def workers_alive():
    """
    REST endpoint to check whether any workers are running

    Checks the RabbitMQ statistics twice, if there is any difference,
    a live worker is assumed.
    """
    global config

    connection_config = config.get("processing_core.msg_broker")
    resp = requests.get(
        f"http://{connection_config['host']}:15672/api/overview",
        auth=(connection_config["username"], connection_config["password"]),
    )
    content = json.loads(resp.content)
    start_stat = content["message_stats"]["deliver_get"]
    time.sleep(2)
    resp = requests.get(
        f"http://{connection_config['host']}:15672/api/overview",
        auth=(connection_config["username"], connection_config["password"]),
    )
    content = json.loads(resp.content)
    end_stat = content["message_stats"]["deliver_get"]
    return json.dumps(
        {
            "workers_alive": end_stat != start_stat,
            "deliver_get_difference": end_stat - start_stat,
        }
    )


if __name__ == "__main__":
    verbose = True
    host = os.getenv("HOST", "127.0.0.1")
    try:
        app.run(host=host)
    except Exception as e:
        print(e)
