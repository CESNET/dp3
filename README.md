## Installing dependencies

`$ pip3 install pipenv`

#### Install all dependencies:

**development**: `$ pipenv install`

**production**: `$ pipenv --ignore-pipfile` 

Generated virtualenv is located at `~/.local/share/virtualenvs`, but should be activated automatically. Deactivate virtualenv with `$ exit`

## Running tests

Activate virtualenv (if not already - `$ pipenv shell`) and run:

`$ cd tests && python3 -m unittest`