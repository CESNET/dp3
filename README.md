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


## Installing dependencies

`$ pip3 install pipenv`

#### Install all dependencies:

**development**: `$ pipenv install`

**production**: `$ pipenv install --ignore-pipfile` 

Generated virtualenv is located at `~/.local/share/virtualenvs`, but should be activated automatically. Deactivate virtualenv with `$ exit`

## Running tests

Activate virtualenv (if not already - `$ pipenv shell`) and run:

`$ cd tests && python3 -m unittest`