from yaml import safe_load
from flask import Flask, request, render_template
from record import Record
from processing_platform.src.task_processing.task_queue import TaskQueueWriter
from processing_platform.src.common.attrspec import load_spec


app = Flask(__name__)
application = app
application.debug = True

# Path to yaml file containing attribute specification
path_attr_spec = "../processing_platform/src/common/attr_spec.yml"

# Path to yaml file containing platform configuration
path_platform_config = "../processing_platform/config/processing_core.yml"

# Dictionary containing attribute specification
# Initialized by AttrSpec.load_spec()
attr_spec = {}

# Dictionary containing platform configuration
# Initialized by init_platform_connection()
platform_config = {}

# TaskQueueWriter instance used for sending tasks to the processing core
global task_writer


# Convert records to tasks and push them to RMQ task queue
def push_records(records):
    tasks = {}
    # Cycle through records and make tasks for each entity
    for r in records:
        k = r["type"], r["id"]
        if k not in tasks.keys():
            tasks[k] = {"data_points": [], "attr_updates": []}

        if attr_spec[r["attr"]].history is True:
            # If history is true, enqueue the record as data point
            tasks[k]["data_points"].append({
                "attr": r["attr"],
                "t1": r["t1"],
                "t2": r["t2"],
                "v": r["v"],
                "c": r["c"]
            })
        else:
            # If history is false, enqueue the record as attribute update
            tasks[k]["attr_updates"].append({
                "op": attr_spec[r["attr"]].attr_update_op,
                "attr": r["attr"],
                "val": r["v"]
            })

    # Enqueue the tasks
    for k in tasks.keys():
        etype, ekey = k
        task_writer.put_task(
            etype,
            ekey,
            tasks[k]["attr_updates"],
            None,
            tasks[k]["data_points"],
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
    # Construct a record from path and query parameters
    r = {
        "type": record_type,
        "id": entity_id,
        "attr": attr_name
    }
    for k in request.args:
        r[k] = request.args[k]

    try:
        # Make valid record using the AttrSpec template and push it to RMQ task queue
        push_records([Record(r, attr_spec)])
        return "Success", 201
    except Exception as e:
        return "Some error(s) occurred:\n" + str(e) + "\n", 400


# REST endpoint to push multiple data points
# Request payload must be a JSON dict containing a list of records
# Example: {"records": [{rec1},{rec2},{rec3},...]}
@app.route("/post", methods=["POST"])
def push_multiple():
    request_json = request.get_json()

    # Request must be valid JSON (dict) and contain a list of records
    if type(request_json) is not dict or \
       "records" not in request_json or \
       type(request_json["records"]) is not list:
        return "Request is not a dict, or does not contain a list of records", 400

    errors = ""
    records = []

    for r in request_json["records"]:
        try:
            # Make valid record using the AttrSpec template
            records.append(Record(r, attr_spec))
        except Exception as e:
            errors += str(e) + "\n"

    # Push records to RMQ task queue
    push_records(records)

    # Set correct response based on the results
    response = "Success", 201
    if errors != "":
        # TODO what status code should we return here?
        response = "Some error(s) occurred:\n" + errors, 202
    return response


# REST endpoint to check whether the API is running
# Returns a simple html template
@app.route("/")
def home():
    return render_template("home.html")


# Load platform configuration and check required fields
def init_platform_connection(path):
    platform_config = safe_load(open(path))
    if "msg_broker" not in platform_config.keys() or \
       "worker_processes" not in platform_config.keys():
        raise KeyError("Invalid platform configuration")
    task_writer = TaskQueueWriter(platform_config["msg_broker"], platform_config["worker_processes"])
    task_writer.connect()


if __name__ == "__main__":
    try:
        # Initialize attribute specification
        attr_spec = load_spec(path_attr_spec)

        # Initialize connection to message broker (RabbitMQ)
        init_platform_connection(path_platform_config)

        # Run the API
        app.run()
    except Exception as e:
        print(e)
