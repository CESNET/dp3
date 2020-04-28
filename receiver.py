import os
import sys
import logging
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../processing_platform')))

from flask import Flask, request, render_template
from record import Record
from src.task_processing.task_queue import TaskQueueWriter
from src.common.attrspec import load_spec
from src.common.config import read_config

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

# Dictionary containing attribute specification
attr_spec = None

# TaskQueueWriter instance used for sending tasks to the processing core
task_writer = None

# Logger
log = None

# Load configuration, initialize logging and connect to platform message broker
def initialize():
    global attr_spec
    global platform_config
    global log

    # Logging initialization
    log_format = "%(asctime)-15s,%(threadName)s,%(name)s,[%(levelname)s] %(message)s"
    log_dateformat = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.DEBUG, format=log_format, datefmt=log_dateformat)
    log = logging.getLogger()

    # Load platform configuration
    platform_config = read_config(path_platform_config)

    # Attribute specification initialization
    try:
        attr_spec = load_spec(path_attr_spec)
    except Exception as e:
        log.error(f"Invalid attribute specification: {str(e)})")

    assert "msg_broker" in platform_config, "configuration does not contain 'msg_broker'"
    assert "worker_processes" in platform_config, "configuration does not contain 'worker_processes'"

    task_writer = TaskQueueWriter(platform_config["worker_processes"], platform_config["msg_broker"])
    task_writer.connect()


# Convert records to tasks and push them to RMQ task queue
def push_records(records):
    global attr_spec
    global task_writer

    tasks = {}
    # Cycle through records and make tasks for each entity
    for r in records:
        key = r["type"], r["id"]
        if key not in tasks:
            tasks[key] = {"data_points": [], "attr_updates": []}

        if attr_spec[r["attr"]].history is True:
            # If history is true, enqueue the record as data point
            tasks[key]["data_points"].append({
                "attr": r["attr"],
                "t1": r["t1"],
                "t2": r["t2"],
                "v": r["v"],
                "c": r["c"]
            })
        else:
            # If history is false, enqueue the record as attribute update
            tasks[key]["attr_updates"].append({
                "op": attr_spec[r["attr"]].attr_update_op,
                "attr": r["attr"],
                "val": r["v"]
            })

    # Enqueue the tasks
    for key in tasks:
        etype, ekey = key
        task_writer.put_task(
            etype,
            ekey,
            tasks[key]["attr_updates"],
            None,
            tasks[key]["data_points"],
            None,
            False,
            "receiver.py",
            None,
            False
        )


# REST endpoint to push a single data point
# Record type, entity id and attribute name are part of the endpoint path
# Other fields should be contained in query parameters
@app.route("/post/<string:record_type>/<string:entity_id>/<string:attr_name>", methods=["POST"])
def push_single(record_type, entity_id, attr_name):
    global attr_spec

    log.info(f"Received new request from {request.remote_addr}")

    # Construct a record from path and query parameters
    r = {
        "type": record_type,
        "id": entity_id,
        "attr": attr_name
    }
    for key in request.args:
        r[key] = request.args[key]

    # Make valid record using the AttrSpec template and push it to RMQ task queue
    try:
        push_records([Record(r, attr_spec)])
        response = "Success"
    except Exception as e:
        response = f"Error: {str(e)}"

    log.info(response)
    return f"{response}\n", 202


# REST endpoint to push multiple data points
# Request payload must be a JSON dict containing a list of records
# Example: {"records": [{rec1},{rec2},{rec3},...]}
@app.route("/post", methods=["POST"])
def push_multiple():
    global attr_spec

    log.info(f"Received new request from {request.remote_addr}")

    # Request must be valid JSON (dict) and contain a list of records
    try:
        request_json = request.get_json()
    except:
        request_json = None

    errors = ""
    if request_json is None:
        errors = "not JSON or empty payload"
    elif type(request_json) is not dict:
        errors = "payload is not a dict"
    elif "records" not in request_json:
        errors = "payload does not contain 'records' field"
    elif type(request_json["records"]) is not list:
        errors = "type of 'records' is not list"

    if (errors != ""):
        # Request is invalid, cannot continue
        response = f"Invalid request: {errors}"
        log.info(response)
        return f"{response}\n", 400

    # Make valid records using the AttrSpec template
    records = []
    for r in request_json["records"]:
        try:
            records.append(Record(r, attr_spec))
        except Exception as e:
            errors += f"\nInvalid data point: {str(e)}"

    # Push records to RMQ task queue
    try:
        push_records(records)
    except Exception as e:
        errors += f"\nFailed to push records: {str(e)}"

    if errors != "":
        response = "Passed with errors:" + errors
    else:
        response = "Success"

    log.info(response)
    return f"{response}\n", 202


# REST endpoint to check whether the API is running
# Returns a simple html template
@app.route("/")
def home():
    return render_template("home.html")


if __name__ == "__main__":
    try:
        # API needs to be initialized before running the web service
        initialize()
        app.run()
    except Exception as e:
        print(e)
