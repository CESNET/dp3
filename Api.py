from flask import Flask, request, render_template
from Record import Record
import SQLiteWrapper
import AttrSpec

app = Flask(__name__)
application = app
application.debug = True

# Dictionary containing attribute specification
# Initialized by AttrSpec.load_spec()
attr_spec = {}


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
        # Make valid data point using the AttrSpec template and send it to SQLite database
        SQLiteWrapper.insert(Record(r, attr_spec))
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

    for r in request_json["records"]:
        try:
            # Make valid data point using the AttrSpec template and send it to SQLite database
            SQLiteWrapper.insert(Record(r, attr_spec))
        except Exception as e:
            errors += str(e) + "\n"

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


if __name__ == "__main__":
    try:
        # Initialize attribute specification
        attr_spec = AttrSpec.load_spec()

        # Initialize SQLite
        SQLiteWrapper.init()

        # Run the API
        app.run()
    except (TypeError, ValueError, ConnectionError) as e:
        print(e)
