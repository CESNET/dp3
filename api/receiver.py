#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
import traceback

import requests
from flask import Flask, request, Response, jsonify

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
from dp3.task_processing.task_queue import TaskQueueWriter
from dp3.common.config import read_config_dir, load_attr_spec
from dp3.database.database import EntityDatabase
from dp3.common.utils import parse_rfc_time
from dp3.history_management.history_manager import get_historic_value

from api.task import Task

app = Flask(__name__)

# Get application name and directory containing config files.
# Can be specified as command-line parameters (when run as stand-alone testing server)
# or as environment variables.
if __name__ == '__main__' and len(sys.argv) > 1:
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument('app_name', help='Identification of DP3 application')
    argparser.add_argument('conf_dir', help='Configuration directory')
    argparser.add_argument('dp_log_file', nargs="?", help='File to store all incoming datapoints (as CSV)')
    args = argparser.parse_args()
    app_name = args.app_name
    conf_dir = args.conf_dir
    dp_log_file = args.dp_log_file
elif 'DP3_CONFIG_DIR' in os.environ and 'DP3_APP_NAME' in os.environ:
    conf_dir = os.environ['DP3_CONFIG_DIR']
    app_name = os.environ['DP3_APP_NAME']
    dp_log_file = os.environ['DP3_DP_LOG_FILE']
else:
    print("Error: DP3_APP_NAME and DP3_CONFIG_DIR environment variables must be set.", file=sys.stderr)
    print("  DP3_APP_NAME - application name used to distinguish this app from other dp3-based apps", file=sys.stderr)
    print("  DP3_CONFIG_DIR - directory containing configuration files", file=sys.stderr)
    print("  DP3_DP_LOG_FILE - (optional) file to store all incoming datapoints (as CSV)", file=sys.stderr)
    sys.exit(1)

# Temporary override
# conf_dir = '/etc/adict/config'
# app_name = 'adict'
# dp_log_file = '/data/datapoints/dp.log'


# Dictionary containing platform configuration
config = None

# Dictionary containing entity / attribute specification
attr_spec = {}

# TaskQueueWriter instance used for sending tasks to the processing core
task_writer = None

# Logger
# log = None

# Flag marking if initialization was successful (no request can be handled if this is False)
initialized = False

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


@app.before_first_request
def initialize():
    """
    Load configuration and connect to dp3 message broker.

    This needs to be called before any request is made.
    """
    global attr_spec
    global config
    global log
    global task_writer
    global initialized
    global db

    # Load configuration and entity/attribute specification
    try:
        config = read_config_dir(conf_dir, recursive=True)
        attr_spec = load_attr_spec(config.get("db_entities"))
        log.debug(f"Loaded configuration: {config}")
    except Exception as e:
        log.exception(f"Error when reading configuration: {e}")
        return  # "initialized" stays False, so any request will fail with Error 500

    # Initialize task queue connection
    try:
        task_writer = TaskQueueWriter(app_name, config.get("processing_core.worker_processes"),
                                      config.get("processing_core.msg_broker"))
    except Exception as e:
        log.exception(f"Error when connecting to task queue: {e}")
        return

    # Initialize database connection
    try:
        db = EntityDatabase(config.get("database"), attr_spec)
    except Exception as e:
        log.exception(f"Error when connecting to database: {e}")
        return

    initialized = True
    log.info("Initialization completed")


@app.before_request
def check_initialization():
    if not initialized:
        return Response("ERROR: Server not correctly initialized, "
                        "probably due to some error in configuration. See server log for details.",
                        500)
    return None  # continue processing request as normal


################################################################################
# Endpoints to write data (push data-points / tasks)

def push_task(task):
    """Push given task (instance of task.Task) to platform's task queue (RabbitMQ)"""
    task_writer.put_task(
        task["etype"],
        task["ekey"],
        task["attr_updates"],
        task["events"],
        task["data_points"],
        task["create"],
        task["delete"],
        task["src"],
        task["tags"],
        task["ttl_token"],
        False  # priority
    )


def convert_value(value, attr_type, data_type):
    try:
        # Timeseries
        if attr_type == "timeseries":
            return value

        # Plain or observations
        if data_type == "tag":
            return True
        elif data_type == "binary":
            parsed_val = json.loads(value)
            if isinstance(parsed_val, bool):
                return parsed_val
            assert parsed_val == 0 or parsed_val == 1
            return bool(parsed_val)
        elif data_type == "int" or data_type == "int64":
            return int(value)
        elif data_type == "float":
            return float(value)
        elif data_type in ("string", "category", "ip4", "ip6", "mac", "timestamp"):  # TODO validate IP/MAC addresses and time?
            return str(value)
        elif data_type.startswith("link<"):
            return str(value)  # TODO: int should also be allowed, depending on what is being linked
        # others: binary, array<>, set<>, dict<>, special
        elif isinstance(value, str):  # passed as string - parse from JSON format
            return json.loads(value)
        else:  # otherwise no conversion is needed
            return value
    except (TypeError, ValueError, AssertionError):
        raise TypeError


def log_datapoints(dps):
    """
    Log incoming datapoint as a line in a CSV file

    CSV fields:
      type, id, attr, t1, t2, c, src, val
    (value is the last to ease parsing, since it can contain commas)

    dps - list of tuples (etype, eid, attr, v, t1, t2, c, src)
    """
    print(dps)
    if not dp_log_file:
        return
    with open(dp_log_file, "a") as f:
        for entity_type, entity_id, attr_id, val, t1, t2, c, src in dps:
            f.write(f"{entity_type},{str(entity_id)},{attr_id},{t1},{t2},{str(c)},{src},{json.dumps(val)}\n")


@app.route("/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["POST"])
def push_single_datapoint(entity_type, entity_id, attr_id):
    """
    REST endpoint to push a single data point (or an attribute update)

    Entity type, id and attribute name are part of the endpoint path.
    Other fields should be contained in query parameters, depending on attribute specification.
    """
    log.debug(f"Received new datapoint from {request.remote_addr}")

    # Extract datapoint fields from the POST part
    raw_val = request.values.get("v", None)
    t1 = request.values.get("t1", None)
    c = request.values.get("c", 1.0)
    src = request.values.get("src", "")

    # Get attribute specification
    try:
        spec = attr_spec[entity_type]["attribs"][attr_id]
    except KeyError:
        response = f"Error: no specification found for {entity_type}/{attr_id}"
        log.info(response)
        return f"{response}\n", 400  # Bad request

    # Get t2
    if spec.type == "timeseries" and spec.timeseries_type == "regular":
        # Length of first series
        series_len = len(list(raw_val.values())[0])

        default_dt = parse_rfc_time(t1) + series_len*spec.time_step
        t2 = request.values.get("t2", default_dt.isoformat("T"))
    else:
        t2 = request.values.get("t2", t1)

    if spec.type == "observations" and t1 is None:
        response = f"Invalid data-point: Missing mandatory field 't1'"
        log.info(response)
        return f"{response}\n", 400  # Bad request

    if attr_spec[entity_type]["entity"].key_data_type == "int":
        try:
            entity_id = int(entity_id)
        except ValueError:
            response = "Error: type of \"entity_id\" is invalid (must be int)"
            log.info(response)
            return f"{response}\n", 400  # Bad request

    # Convert value from string (JSON) to proper data type
    try:
        val = convert_value(raw_val, spec.type, spec.data_type)
    except TypeError:
        response = f'Error: type of "v" is invalid ("{raw_val}" is not {spec.data_type})'
        log.info(response)
        return f"{response}\n", 400  # Bad request

    # Convert confidence to float
    try:
        c = float(c)
    except ValueError:
        response = f'Error: type of "c" is invalid ("{c}" is not float)'
        log.info(response)
        return f"{response}\n", 400  # Bad request

    if spec.probability:
        try:
            val = json.loads(val)
        except (TypeError, ValueError):
            response = f"Error: \"v\" is not a probability distribution (JSON loads failed)\n"
            log.info(f"{response}Value: {val}")
            return response, 400  # Bad request
        if not spec.value_validator(val):
            response = f"Error: \"v\" is not a probability distribution (format invalid)\n"
            log.info(f"{response}Value: {val}")
            return response, 400  # Bad request

    # Log the datapoint
    try:
        log_datapoints([(entity_type, entity_id, attr_id, val, t1, t2, c, src)])
    except Exception as e:
        print("ERROR: Can't log datapoint:", e, file=sys.stderr)

    if spec.probability:
        response = "Success"
        log.debug(response)
        return f"{response}\n", 200  # OK

    # Prepare task
    t = {
        "etype": entity_type,
        "ekey": entity_id,
        "src": src,
        "ttl_token": "default"
    }

    if spec.type == "observations" or spec.type == "timeseries":
        t["data_points"] = [{
            "attr": attr_id,
            "v": val,
            "t1": t1,
            "t2": t2,
            "c": c,
            "src": src
        }]
    else:
        t["attr_updates"] = [{
            "attr": attr_id,
            "op": "set",
            "val": val
        }]

    # Make valid task using the attr_spec template and push it to platform's task queue
    try:
        task = Task(t, attr_spec)
    except Exception as e:
        traceback.print_exc()
        response = f"Error: Failed to create a task: {type(e)}: {str(e)}"
        log.info(response)
        return f"{response}\n", 400  # Bad request
    try:
        push_task(task)
    except Exception as e:
        traceback.print_exc()
        response = f"Error: Failed to push task: {type(e)}: {str(e)}"
        log.error(response)
        return f"{response}\n", 500  # Internal server error

    response = "Success"
    log.debug(response)
    return f"{response}\n", 200  # OK


@app.route("/datapoints", methods=["POST"])
def push_multiple_datapoints():
    """
    REST endpoint to push multiple data points

    Request payload must be a JSON dict containing a list of datapoints.
    Example:
        [{dp1},{dp2},{dp3},...]
    """
    log.debug(f"Received new datapoint(s) from {request.remote_addr}")

    # Request must be valid JSON (dict) and contain a list of records
    try:
        payload = request.get_json(force=True)  # force = ignore mimetype
    except Exception:
        payload = None

    errors = ""
    if payload is None:
        errors = "not a valid JSON (or empty payload)"
    elif type(payload) is not list:
        errors = "payload is not a list"

    if errors != "":
        # Request is invalid, cannot continue
        response = f"Invalid request: {errors}"
        log.info(response)
        return f"{response}\n", 400  # Bad request

    # Load all datapoints from POST data
    dps = []
    for i, record in enumerate(payload, 1):
        # Check it's a dict
        if type(record) is not dict:
            response = f"Invalid data-point no. {i}: Not a dictionary"
            log.info(f"{response}\nRecord: {record}")
            return f"{response}\n", 400

        # Extract fields
        try:
            etype, ekey, attr = record["type"], record["id"], record["attr"]
        except KeyError as e:
            response = f"Invalid data-point no. {i}: Missing field '{str(e)}'"
            log.info(f"{response}\nRecord: {record}")
            return f"{response}\n", 400
        raw_val = record.get("v", None)
        c = record.get("c", 1.0)
        src = record.get("src", "")

        # Get attribute specification
        try:
            spec = attr_spec[etype]["attribs"][attr]
        except KeyError:
            response = f"Error: no specification found for {etype}/{attr}"
            log.info(f"{response}\nRecord: {record}")
            return f"{response}\n", 400  # Bad request

        # Get t1
        if spec.type in ["observations", "timeseries"]:
            try:
                t1 = record["t1"]
            except KeyError as e:
                response = f"Invalid data-point no. {i}: Missing field '{str(e)}'"
                log.info(f"{response}\nRecord: {record}")
                return f"{response}\n", 400

        # Get t2
        if spec.type == "timeseries":
            t2 = record.get("t2", None)
            if t2 is None:
                if spec.timeseries_type == "regular":
                    series_len = len(list(raw_val.values())[0]) # length of first series
                    t2 = parse_rfc_time(t1) + series_len * spec.time_step
                elif spec.timeseries_type == "irregular":
                    t2 = parse_rfc_time(raw_val["time"][-1]) # set t2 to last timestamp
                elif spec.timeseries_type == "irregular_intervals":
                    t2 = parse_rfc_time(raw_val["time_last"][-1]) # set t2 to last end timestamp
        elif spec.type == "observations":
            t2 = record.get("t2", t1)

        # Convert value from string (JSON) to proper data type
        try:
            value = convert_value(raw_val, spec.type, spec.data_type)
        except TypeError:
            response = f'Error: type of "v" is invalid ("{raw_val}" is not {spec.data_type})'
            log.info(f"{response}\nRecord: {record}")
            return f"{response}\n", 400  # Bad request

        # Convert confidence to float
        try:
            c = float(c)
        except ValueError:
            response = f'Error: type of "c" is invalid ("{c}" is not float)'
            log.info(response)
            return f"{response}\n", 400  # Bad request

        if spec.probability:
            try:
                val = json.loads(value)
            except (TypeError, ValueError):
                response = f"Error: \"v\" is not a probability distribution (JSON loads failed)\n"
                log.info(f"{response}Value: {value}")
                return response, 400  # Bad request
            if not spec.value_validator(val):
                response = f"Error: \"v\" is not a probability distribution (format invalid)\n"
                log.info(f"{response}Value: {val}")
                return response, 400  # Bad request

        if spec.type in ["observations", "timeseries"]:
            dps.append((etype, ekey, attr, value, t1, t2, c, src, spec))
        else:  # spec.type == "plain"
            dps.append((etype, ekey, attr, value, None, None, c, src, spec))


    # Log all datapoints (regardless of their validity)
    try:
        log_datapoints([dp[:8] for dp in dps])  # pass all params except the last one (attr spec)
    except Exception as e:
        print("ERROR: Can't log datapoints:", e, file=sys.stderr)

    # Create a task for each (etype,ekey) in data-points
    tasks = {}
    for etype, ekey, attr, value, t1, t2, c, src, spec in dps:
        if spec.probability:
            continue

        key = (etype, ekey)
        if key not in tasks:
            # create new "empty" task
            tasks[key] = {
                "etype": etype,
                "ekey": ekey,
                "data_points": [],
                "attr_updates": [],
                "src": src,
                "ttl_token": "default"
            }

        # Add data-points or attr updates
        if spec.type == "observations" or spec.type == "timeseries":
            tasks[key]["data_points"].append({
                "attr": attr,
                "v": value,
                "t1": t1,
                "t2": t2,
                "c": c,
                "src": src
            })
        else:
            tasks[key]["attr_updates"].append({
                "attr": attr,
                "op": "set",
                "val": value
            })

    task_list = []

    # Make valid tasks using the attr_spec template
    for k in tasks:
        try:
            task_list.append(Task(tasks[k], attr_spec))
        except Exception as e:
            # traceback.print_exc()
            response = f"\nFailed to create a task: {type(e)}: {str(e)}"
            log.info(response)
            return f"{response}\n", 400  # Bad request

    # Push valid tasks to platform task queue
    for task in task_list:
        try:
            push_task(task)
        except Exception as e:
            traceback.print_exc()
            response = f"\nFailed to push task: {type(e)}: {str(e)}"
            log.error(response)
            return f"{response}\n", 500  # Internal server error

    response = "Success"
    log.debug(response)
    return f"{response}\n", 200  # OK


@app.route("/tasks", methods=["POST"])
def push_single_task():
    """
    REST endpoint to push a single task

    Task structure (JSON) should be contained directly in request payload.
    """
    log.debug(f"Received new task from {request.remote_addr}")

    # Request must be valid JSON (dict)
    try:
        payload = request.get_json(force=True)  # force = ignore mimetype
    except Exception:
        payload = None

    errors = ""
    if payload is None:
        errors = "not a valid JSON (or empty payload)"
    elif type(payload) is not dict:
        errors = "payload is not a dict"

    if errors != "":
        # Request is invalid, cannot continue
        response = f"Invalid request: {errors}"
        log.info(response)
        return f"{response}\n", 400  # Bad request

    # Make valid task and push it to platforms task queue
    try:
        task = Task(payload, attr_spec)
    except Exception as e:
        traceback.print_exc()
        response = f"Error: Failed to create a task: {str(e)}"
        log.info(response)
        return f"{response}\n", 400  # Bad request
    try:
        push_task(task)
    except Exception as e:
        traceback.print_exc()
        response = f"Error: Failed to push task to queue: {str(e)}"
        log.error(response)
        return f"{response}\n", 500  # Internal server error

    response = "Success"
    log.debug(response)
    return f"{response}\n", 200  # OK


################################################################################
# Endpoints to read data

@app.route("/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["GET"])
def get_attr_value(entity_type, entity_id, attr_id):
    """
    REST endpoint to read current value for an attribute of given entity

    It is also possible to read historic values by providing a specific timestamp as a query parameter (only for observations)

    Entity type, entity id and attribute id must be provided

    Examples:
        /ip/1.2.3.4/test_attr
        /ip/1.2.3.4/test_attr?t=2000-01-01T00:00:00
    """
    log.debug(f"Received new GET request from {request.remote_addr}")

    try:
        _ = attr_spec[entity_type]["attribs"][attr_id]
    except KeyError:
        response = f"Error: no specification found for {entity_type}/{attr_id}"
        log.info(response)
        return f"{response}\n", 400  # Bad request

    try:
        timestamp = request.values.get("t", None)
        observations = attr_spec[entity_type]['attribs'][attr_id].type == "observations"

        if timestamp is not None and observations:
            content = get_historic_value(db, attr_spec, entity_type, entity_id, attr_id, parse_rfc_time(timestamp))
        else:
            content = db.get_attrib(entity_type, entity_id, attr_id)

        if content is None:
            response = f"No records found for {entity_type}/{entity_id}/{attr_id}", 404  # Not found
        else:
            response = jsonify(content), 200  # OK
    except Exception as e:
        response = f"Error when querying db: {e}", 500  # Internal server error

    return response


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
    log.debug(f"Received new GET request from {request.remote_addr}")

    try:
        f = attr_spec[entity_type]["entity"].key_validator
    except KeyError:
        return f"Error: no entity specification found for '{entity_type}'", 400  # Bad request
    try:
        _ = attr_spec[entity_type]["attribs"][attr_id]
    except KeyError:
        return f"Error: no attribute specification found for '{attr_id}'", 400  # Bad request
    if not f(entity_id):
        return "Error: invalid entity id", 400  # Bad request

    t1 = request.args.get("t1", None)
    t2 = request.args.get("t2", None)
    if t1 is not None:
        try:
            _t1 = parse_rfc_time(t1)
        except ValueError:
            return "Error: invalid timestamp format (t1)", 400  # Bad request
    if t2 is not None:
        try:
            _t2 = parse_rfc_time(t2)
        except ValueError:
            return "Error: invalid timestamp format (t2)", 400  # Bad request
    if t1 is not None and \
       t2 is not None and \
       _t2 < _t1:
        return "Error: invalid time interval (t2 < t1)", 400 # Bad request

    try:
        content = db.get_datapoints_range(entity_type, attr_id, entity_id, t1, t2)
        if content is None:
            response = f"No records found for {entity_type}/{entity_id}/{attr_id}", 404  # Not found
        else:
            response = jsonify(content), 200  # OK
    except Exception as e:
        response = f"Error when querying db: {e}", 500  # Internal server error

    return response


@app.route("/", methods=["GET"])
def ping():
    """
    REST endpoint to check whether the API is running

    Returns a simple text response ("It works!")
    """
    return Response("It works!", status=200, mimetype="text/plain")


@app.route("/workers_alive", methods=["GET"])
def workers_alive():
    """
    REST endpoint to check whether any workers are running

    Checks the RabbitMQ statistics twice, if there is any difference,
    a live worker is assumed.
    """
    global config

    connection_config = config.get("processing_core.msg_broker")
    resp = requests.get(f"http://{connection_config['host']}:15672/api/overview",
                        auth=(connection_config['username'], connection_config['password']))
    content = json.loads(resp.content)
    start_stat = content["message_stats"]["deliver_get"]
    time.sleep(2)
    resp = requests.get(f"http://{connection_config['host']}:15672/api/overview",
                        auth=(connection_config['username'], connection_config['password']))
    content = json.loads(resp.content)
    end_stat = content["message_stats"]["deliver_get"]
    return json.dumps({"workers_alive": not (end_stat == start_stat),
                       "deliver_get_difference": end_stat - start_stat})


if __name__ == "__main__":
    verbose = True
    host = os.getenv("HOST", "127.0.0.1")
    try:
        app.run(host=host)
    except Exception as e:
        print(e)
