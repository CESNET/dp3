import os
import sys
import logging
import yaml
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../processing_platform')))

from flask import Flask, request, render_template
from task import Task
from src.task_processing.task_queue import TaskQueueWriter
from src.common.config import read_config, load_attr_spec

app = Flask(__name__)
application = app
application.debug = True

# Directory containing config files
conf_dir = "/ADiCT/processing_platform/config"

# Path to yaml file containing attribute specification
path_attr_spec = f"{conf_dir}/attributes_specification.yml"

# Path to yaml file containing platform configuration
path_platform_config = f"{conf_dir}/processing_core.yml"

# Dictionary containing platform configuration
platform_config = None

# Dictionary containing entity / attribute specification
attr_spec = None

# TaskQueueWriter instance used for sending tasks to the processing core
task_writer = None

# Logger
log = None

# Load configuration, initialize logging and connect to platform message broker
# This need to be called before any request is made
@app.before_first_request
def initialize():
    global attr_spec
    global platform_config
    global log
    global task_writer

    # Logging initialization
    log_format = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    log_dateformat = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.WARNING, format=log_format, datefmt=log_dateformat)
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    # Load configuration and entity/attribute specification
    try:
        platform_config = read_config(path_platform_config)
        attr_spec = load_attr_spec(yaml.safe_load(open(path_attr_spec)))
    except Exception as e:
        log.error(str(e))
        return
        # TODO what to do here?

    assert "msg_broker" in platform_config, "configuration does not contain 'msg_broker'"
    assert "worker_processes" in platform_config, "configuration does not contain 'worker_processes'"

    task_writer = TaskQueueWriter(platform_config["worker_processes"], platform_config["msg_broker"])
    log.info("Initialization completed")


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
        config = attr_spec[entity_type]["attribs"][attr_id]
    except Exception as e:
        response = f"Error: {str(e)}"
        log.info(response)
        return f"{response}\n", 400

    t = {
        "etype": entity_type,
        "ekey": entity_id,
        "src": request.args.get("src", "")
    }

    if config.history is True:
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
        push_task(Task(t, attr_spec))
        response = "Success"
    except Exception as e:
        response = f"Error: {str(e)}"

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
            config = attr_spec[record["type"]]["attribs"][record["attr"]]
            if config.history is True:
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
            push_task(Task(tasks[k], attr_spec))
        except Exception as e:
            errors += f"\nFailed to push task: {str(e)}"

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
        push_task(Task(payload, attr_spec))
    except Exception as e:
        errors += f"\nFailed to push task: {str(e)}"

    if errors != "":
        response = f"Error: {errors}"
    else:
        response = "Success"

    log.info(response)
    return f"{response}\n", 202


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
