import os
import sys
import logging
import traceback
from flask import Flask, request, render_template, Response, jsonify

sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../processing_platform')))
from src.task_processing.task_queue import TaskQueueWriter
from src.common.config import read_config_dir, load_attr_spec
from src.database.database import EntityDatabase
from src.common.utils import parse_rfc_time

from task import Task

app = Flask(__name__)
application = app
application.debug = True

# Directory containing config files
# TODO this should be configurable somehow (env var passed from Apache config?)
conf_dir = "/etc/adict/config"

# Dictionary containing platform configuration
config = None

# Dictionary containing entity / attribute specification
attr_spec = None

# TaskQueueWriter instance used for sending tasks to the processing core
task_writer = None

# Logger
log = None

# Flag marking if initialization was successful (no request can be handled if this is False)
initialized = False

# Database handler
db = None


@app.before_first_request
def initialize():
    """
    Load configuration, initialize logging and connect to platform message broker.

    This needs to be called before any request is made.
    """
    global attr_spec
    global config
    global log
    global task_writer
    global initialized
    global db

    # Logging initialization
    log_format = "%(asctime)-15s [%(levelname)s] %(message)s"
    log_dateformat = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.WARNING, format=log_format, datefmt=log_dateformat)
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    # Load configuration and entity/attribute specification
    try:
        config = read_config_dir(conf_dir, recursive=True)
        attr_spec = load_attr_spec(config.get("db_entities"))
        log.info(f"Loaded configuration: {config}")
    except Exception as e:
        log.exception(f"Error when reading configuration: {e}")
        return # "initialized" stays False, so any request will fail with Error 500

    # Initialize task queue connection
    try:
        task_writer = TaskQueueWriter(config.get("processing_core.worker_processes"), config.get("processing_core.msg_broker"))
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
        return Response("ERROR: Server not correctly initialized, probably due to some error in configuration. See server log for details.", 500)
    return None # continue processing request as normal


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
        False # priority
    )


@app.route("/datapoints/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["POST"])
def push_single_datapoint(entity_type, entity_id, attr_id):
    """
    REST endpoint to push a single data point

    Entity type, id and attribute name are part of the endpoint path.
    Other fields should be contained in query parameters.
    """
    log.debug(f"Received new datapoint from {request.remote_addr}")

    try:
        spec = attr_spec[entity_type]["attribs"][attr_id]
    except Exception as e:
        response = f"Error: {str(e)}"
        log.debug(response)
        return f"{response}\n", 400 # Bad request

    t = {
        "etype": entity_type,
        "ekey": entity_id,
        "src": request.args.get("src", "")
    }

    if spec.history is True:
        t["data_points"] = [{
            "attr": attr_id,
            **request.args # TODO check for excess fields?
        }]
    else:
        t["attr_updates"] = [{
            "attr": attr_id,
            "op": "set",
            "val": request.args.get("v", None)
        }]

    # Make valid task using the attr_spec template and push it to platform's task queue
    try:
        task = Task(t, attr_spec)
    except Exception as e:
        response = f"Error: Failed to create a task: {str(e)}"
        log.info(response)
        return f"{response}\n", 400 # Bad request
    try:
        push_task(task)
    except Exception as e:
        response = f"Error: Failed to push task to queue: {str(e)}"
        log.info(response)
        return f"{response}\n", 500 # Internal server error

    response = "Success"
    log.debug(response)
    return f"{response}\n", 202 # Accepted


@app.route("/datapoints", methods=["POST"])
def push_multiple_datapoints():
    """
    REST endpoint to push multiple data points

    Request payload must be a JSON dict containing a list of records.
    Example:
        {"records": [{rec1},{rec2},{rec3},...]}
    """
    log.debug(f"Received new datapoint(s) from {request.remote_addr}")

    # Request must be valid JSON (dict) and contain a list of records
    try:
        payload = request.get_json(force=True) # force = ignore mimetype
    except Exception:
        payload = None

    errors = ""
    if payload is None:
        errors = "not JSON or empty payload"
    elif type(payload) is not list:
        errors = "payload is not a list"

    if errors != "":
        # Request is invalid, cannot continue
        response = f"Invalid request: {errors}"
        log.info(response)
        return f"{response}\n", 400 # Bad request

    # Create a task for each (etype,ekey) in data-points
    tasks = {}
    for i,record in enumerate(payload, 1):
        # Extract all mandatory fields
        try:
            etype, ekey, attr = record["type"], record["id"], record["attr"]
            _ = record["t1"] # try to access just to check field existence
        except KeyError as e:
            response = f"Invalid data-point no. {i}: Missing field '{str(e)}'"
            log.info(response)
            return f"{response}\n", 400

        key = (etype, ekey)
        if key not in tasks:
            # create new "empty" task
            tasks[key] = {
                "etype": etype,
                "ekey": ekey,
                "data_points": [],
                "attr_updates": [],
                "src": record.get("src", "")
            }

        # add data-points or attr updates
        spec = attr_spec[etype]["attribs"][attr]
        if spec.history is True:
            tasks[key]["data_points"].append(record)
        else:
            tasks[key]["attr_updates"].append({
                "attr": attr,
                "op": "set",
                "val": record.get("v", None)
            })

    # Make valid tasks using the attr_spec template and push it to platform's task queue
    for k in tasks:
        try:
            task = Task(tasks[k], attr_spec)
        except Exception as e:
            traceback.print_exc()
            errors += f"\nFailed to create a task: {str(e)}"
            continue
        try:
            push_task(task)
        except Exception as e:
            traceback.print_exc()
            errors += f"\nFailed to push task: {str(e)}"

    if errors != "":
        response = f"Error: {errors}"
        log.info(response)
    else:
        response = "Success"
        log.debug(response)

    return f"{response}\n", 202 # Accepted (what status code to return when there is error in *some* of the tasks?)


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
        errors = "not JSON or empty payload"
    elif type(payload) is not dict:
        errors = "payload is not a dict"

    if errors != "":
        # Request is invalid, cannot continue
        response = f"Invalid request: {errors}"
        log.info(response)
        return f"{response}\n", 400 # Bad request

    # Make valid task and push it to platforms task queue
    try:
        task = Task(payload, attr_spec)
    except Exception as e:
        response = f"Error: Failed to create a task: {str(e)}"
        log.info(response)
        return f"{response}\n", 400 # Bad request
    try:
        push_task(task)
    except Exception as e:
        response = f"Error: Failed to push task to queue: {str(e)}"
        log.info(response)
        return f"{response}\n", 500 # Internal server error

    response = "Success"
    log.debug(response)
    return f"{response}\n", 202 # Accepted


################################################################################
# Endpoints to read data

# TODO: why is this under /datapoints/, when it's about current value? (datapoints are more about history)
#       What about the second option?
@app.route("/datapoints/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["GET"])
@app.route("/get/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["GET"])
def get_attribute(entity_type, entity_id, attr_id):
    """
    REST endpoint to read current value for an attribute of given entity

    Entity type, entity id and attribute id must be provided
    """
    log.debug(f"Received new GET request from {request.remote_addr}")

    try:
        content = db.get_attrib(entity_type, entity_id, attr_id)
        if content is None:
            response = f"No records found for {entity_type}/{entity_id}/{attr_id}", 404  # Not found
        else:
            response = jsonify(content), 200 # OK
    except Exception as e:
        response = f"Error when querying db: {e}", 500 # Internal server error

    return response


@app.route("/datapoints/<string:entity_id>/<string:attr_id>", methods=["GET"])
def get_datapoints_range(entity_id, attr_id):
    """
    REST endpoint to read data points (of one attribute) from given time interval

    Attribute id is mandatory
    Timestamps t1, t2 are optional and should be contained in query parameters

    Example:
        /datapoints/test_attr?t1=2020-01-23T12:00:00&t2=2020-01-23T14:00:00

    # TODO:
    # Should entity type be specified as well? It's needed for entity_id data type validation.
    # Also db method for reading data points currently only accepts string as entity id data type
    """
    log.debug(f"Received new GET request from {request.remote_addr}")

    time_min = "" # TODO earliest possible timestamp
    time_max = "" # TODO latest possible timestamp
    # TODO: it will be better to set it to None and modify db method to not include such times into WHERE clause

    t1 = request.args.get("t1", time_min)
    t2 = request.args.get("t2", time_max)

    try:
        _t1 = parse_rfc_time(t1)
        _t2 = parse_rfc_time(t2)
    except ValueError:
        return "Error: invalid timestamp format", 400 # Bad request

    if _t2 < _t1:
        return "Error: invalid time interval (t2 < t1)", 400 # Bad request

    try:
        content = db.get_datapoints_range(attr_id, entity_id, t1, t2)
        if content is None:
            response = f"No records found for {entity_id}/{attr_id}", 404 # Not found
        else:
            response = jsonify(content), 200 # OK
    except Exception as e:
        response = f"Error when querying db: {e}", 500 # Internal server error

    return response


@app.route("/")
def ping():
    """
    REST endpoint to check whether the API is running

    Returns a simple html template
    """
    return render_template("ping.html")


if __name__ == "__main__":
    try:
        app.run()
    except Exception as e:
        print(e)
