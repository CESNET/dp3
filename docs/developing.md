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

## Running tests

Activate virtualenv (if not already - `$ source venv/bin/activate`) and run:

`$ cd tests && python3 -m unittest`
