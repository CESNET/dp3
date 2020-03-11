from flask import Flask, request, render_template, abort
import AttrSpec
import SQLiteWrapper

app = Flask(__name__)
application = app
application.debug = True

# Path to yaml file containing attribute specification
attrspec_path = "PATH TO ATTRSPEC FILE"


# REST endpoint to push a single data point
# Record type, entity id and attribute name are part of the endpoint path
# Other fields should be contained in query parameters
@app.route("/post/<string:record_type>/<string:entity_id>/<string:attr_name>", methods=["POST"])
def push_single(record_type, entity_id, attr_name):
    # Construct a record from endpoint path and query parameters
    d = {
        "type": record_type,
        "id": entity_id,
        "attr": attr_name
    }
    for k in request.args:
        d[k] = request.args[k]

    # Make valid data point using the AttrSpec template and send it to SQLite database
    try:
        record = AttrSpec.make_record(d)
        SQLiteWrapper.insert(record)
        return "Success", 201
    except:
        return "Invalid record", 400


# REST endpoint to push multiple data points
# Request payload must be a JSON dict containing a list of records
# Example: {"records": [{rec1},{rec2},{rec3},...]}
@app.route("/post", methods=["POST"])
def push_multiple():
    invalid = 0
    request_json = request.get_json()

    # Request must be valid JSON (dict) and contain a list of records
    if type(request_json) is not dict or \
       "records" not in request_json or \
       type(request_json["records"]) is not list:
        abort(400)

    # Make valid data points using the AttrSpec template and send them to SQLite database
    for r in request_json["records"]:
        try:
            record = AttrSpec.make_record(r)
            SQLiteWrapper.insert(record)
        except:
            invalid += 1

    # Set correct response based on the results
    response = "Success"
    if invalid > 0:
        response = "Skipped {} of {} records.\n".format(invalid, len(request_json["records"]))
    return response, 201


# REST endpoint to check whether the API is running
# Returns a simple html template
@app.route("/")
def home():
    return render_template("home.html")


if __name__ == "__main__":
    try:
        # Initialize attribute specification
        AttrSpec.load_yaml(attrspec_path)

        # Initialize SQLite
        SQLiteWrapper.init()

        # Run the API
        app.run()
    except:
        print("Failed to initialize")
