# Dynamic Profile Processing Platform (DP³)

DP³ is a platform helps to keep a database of information (attributes) about individual
entities (designed for IP addresses and other network identifiers, but may be anything),
when the data constantly changes in time.

TODO - more description of basic principles and goals

This is a basis of CESNET's "Asset Discovery Classification and Tagging" (ADiCT) project,
focused on discovery and classification of network devices,
but the platform itself is general and should be usable for any kind of data.

DP³ doesn't do much by itself, it must be supplemented by application-specific modules providing
and processing data.

TODO - architecture, main parts of repository (Python package, other files)

## Repository structure

* `dp3` - Python package containing code of the processing core (?)
* `api` - HTTP API (Flask) for data ingestion and extraction
* `config` - default/example configuration
* `install` - installation scripts and files

See the [documentation](https://cesnet.github.io/dp3/) for more details.

## Installing for development

### Dependencies

Following steps create a virtual environment with all dependencies installed.

```shell
python3.9 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pre-commit install
```

The `venv` is activated using `source venv/bin/activate`, and can be deactivated using `deactivate`.

### Using pre-commit

With the dependencies, the [pre-commit](https://pre-commit.com/) package is installed.
You can verify the installation using `pre-commit --version`.
Pre-commit is used to automatically unify code formatting and perform code linting.
The hooks configured in `.pre-commit-config.yaml` should now run automatically on every commit.

In case you want to make sure, you can run `pre-commit run --all-files` to see it in action.

## Installing DP³ platform

`$ pip install -e .`

## Running tests

Activate virtualenv (if not already - `$ source venv/bin/activate`) and run:

`$ cd tests && python3 -m unittest`
