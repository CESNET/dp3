import os
import sys
import logging
import traceback
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../processing_platform')))

from flask import Flask, request, render_template, Response
from task import Task
from src.task_processing.task_queue import TaskQueueWriter
from src.common.config import read_config_dir, load_attr_spec
from src.database.database import EntityDatabase
from src.common.utils import parse_rfc_time

app = Flask(__name__)
application = app
application.debug = True

# Directory containing config files
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


# Load configuration, initialize logging and connect to platform message broker
# This need to be called before any request is made
@app.before_first_request
def initialize():
    global attr_spec
    global config
    global log
    global task_writer
    global initialized
    global db

    # Logging initialization
    log_format = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    log_dateformat = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.WARNING, format=log_format, datefmt=log_dateformat)
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    # Load configuration and entity/attribute specification
    try:
        config = read_config_dir(conf_dir, recursive=True)
        attr_spec = load_attr_spec(config["db_entities"])
        log.info(f"Loaded configuration: {config}")
    except Exception as e:
        log.exception(f"Error when reading configuration: {e}")
        return # "initialized" stays False, so any request will fail with Error 500

    # Initialize task queue connection
    try:
        task_writer = TaskQueueWriter(config["processing_platform"]["worker_processes"], config["processing_platform"]["msg_broker"])
    except Exception as e:
        log.exception(f"Error when connecting to task queue: {e}")
        return

    # Initialize database connection
    try:
        db = EntityDatabase(config["database"])
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


# Push given task to platforms task queue (RabbitMQ)
def push_task(task):
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
            False
        )


# REST endpoint to push a single data point
# Entity type, id and attribute name are part of the endpoint path
# Other fields should be contained in query parameters
@app.route("/datapoints/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["POST"])
def push_single_datapoint(entity_type, entity_id, attr_id):
    log.info(f"Received new datapoint from {request.remote_addr}")

    try:
        spec = attr_spec[entity_type]["attribs"][attr_id]
    except Exception as e:
        response = f"Error: {str(e)}"
        log.info(response)
        return f"{response}\n", 400

    t = {
        "etype": entity_type,
        "ekey": entity_id,
        "src": request.args.get("src", "")
    }

    if spec.history is True:
        t["data_points"] = [{
            "attr": attr_id
        }]
        for k in request.args:
            t["data_points"][0][k] = request.args[k]
    else:
        t["attr_updates"] = [{
            "attr": attr_id,
            "op": "set",
            "val": request.args.get("v", None)
        }]

    # Make valid task using the attr_spec template and push it to platforms task queue
    try:
        task = Task(t, attr_spec)
        try:
            push_task(task)
            response = "Success"
        except Exception as e:
            response = f"Error: Failed to push task: {str(e)}"
    except Exception as e:
        response = f"Error: Failed to create a task: {str(e)}"

    log.info(response)
    return f"{response}\n", 202


# REST endpoint to push multiple data points
# Request payload must be a JSON dict containing a list of records
# Example: {"records": [{rec1},{rec2},{rec3},...]}
@app.route("/datapoints", methods=["POST"])
def push_multiple_datapoints():
    log.info(f"Received new datapoint(s) from {request.remote_addr}")

    # Request must be valid JSON (dict) and contain a list of records
    try:
        payload = request.get_json(force=True) # force = ignore mimetype
    except:
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
        return f"{response}\n", 400

    tasks = {}
    for record in payload:
        key = record["type"], record["id"]
        if key not in tasks:
            tasks[key] = {
                "etype": record["type"],
                "ekey": record["id"],
                "data_points": [],
                "attr_updates": [],
                "src": record.get("src", "")
            }

        try:
            spec = attr_spec[record["type"]]["attribs"][record["attr"]]
            if spec.history is True:
                tasks[key]["data_points"].append(record)
            else:
                tasks[key]["attr_updates"].append({
                    "attr": record["attr"],
                    "op": "set",
                    "val": record.get("v", None)
                })
        except Exception as e:
            response = f"Invalid data-point: {str(e)}"
            log.info(response)
            return f"{response}\n", 400

    # Make valid tasks using the attr_spec template and push it to platforms task queue
    for k in tasks:
        try:
            task = Task(tasks[k], attr_spec)
            try:
                push_task(task)
            except Exception as e:
                traceback.print_exc()
                errors += f"\nFailed to push task: {str(e)}"
        except Exception as e:
            traceback.print_exc()
            errors += f"\nFailed to create a task: {str(e)}"


    if errors != "":
        response = f"Error: {errors}"
    else:
        response = "Success"

    log.info(response)
    return f"{response}\n", 202


# REST endpoint to push a single task
# Task structure (JSON) should be contained directly in request payload
@app.route("/tasks", methods=["POST"])
def push_single_task():
    log.info(f"Received new task from {request.remote_addr}")

    # Request must be valid JSON (dict)
    try:
        payload = request.get_json(force=True)  # force = ignore mimetype
    except:
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
        return f"{response}\n", 400

    # Make valid task and push it to platforms task queue
    try:
        task = Task(payload, attr_spec)
        try:
            push_task(task)
        except Exception as e:
            errors += f"\nFailed to push task: {str(e)}"
    except Exception as e:
        errors += f"\nFailed to create a task: {str(e)}"

    if errors != "":
        response = f"Error: {errors}"
    else:
        response = "Success"

    log.info(response)
    return f"{response}\n", 202


# REST endpoint to read current value for an attribute of given entity
# Entity type, entity id and attribute id must be provided
@app.route("/datapoints/<string:entity_type>/<string:entity_id>/<string:attr_id>", methods=["GET"])
def get_attribute(entity_type, entity_id, attr_id):
    log.info(f"Received new GET request from {request.remote_addr}")

    try:
        response = db.get_attrib(entity_type, entity_id, attr_id), 200
    except Exception as e:
        response = f"Error when querying db: {e}", 500

    return response


# REST endpoint to read data points (of one attribute) from given time interval
# Attribute id is mandatory
# Timestamps t1, t2 are optional and should be contained in query parameters
@app.route("/datapoints/<string:attr_id>", methods=["GET"])
def get_datapoints_range(attr_id):
    log.info(f"Received new GET request from {request.remote_addr}")

    time_min = "" # TODO earliest possible timestamp
    time_max = "" # TODO latest possible timestamp

    t1 = request.args.get("t1", None)
    t2 = request.args.get("t2", None)

    if t1 is None:
        t1 = time_min
    if t2 is None:
        t2 = time_max

    try:
        _t1 = parse_rfc_time(t1)
        _t2 = parse_rfc_time(t2)
    except ValueError:
        return "Error: invalid timestamp format", 400

    if _t1 < _t2:
        return "Error: invalid time interval (t2 < t1)", 400

    try:
        response = db.get_datapoints_range(attr_id, t1, t2), 200
    except Exception as e:
        response = f"Error when querying db: {e}", 500

    return response


# REST endpoint to check whether the API is running
# Returns a simple html template
@app.route("/")
def ping():
    return render_template("ping.html")


if __name__ == "__main__":
    try:
        app.run()
    except Exception as e:
        print(e)
