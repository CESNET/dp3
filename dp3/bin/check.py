"""
Load and check configuration from given directory, print any errors, and exit.

TODO:
 - refactor to simplify the code, some error path matching must be done to counteract
   the AttrSpec function magic where Pydantic fails, but otherwise it is not required
 - integrate to the DP3 executable when ready
 - some errors are printed to stdout, some to stderr - unify
 - integrate code into worker and api to get better errors at startup
"""

import argparse
import json
import sys
from typing import Optional, get_args, get_origin, get_type_hints

import yaml
from pydantic import BaseModel, ValidationError

from dp3.common.attrspec import AttrSpec
from dp3.common.config import EntitySpecDict, ModelSpec, read_config_dir

# Parse arguments
parser = argparse.ArgumentParser(
    prog="check_config",
    description="Load configuration from given directory and check its validity. "
    "When configuration is OK, program exits immediately with status code 0, "
    "otherwise it prints error messages on stderr and exits with non-zero status.",
)
parser.add_argument(
    "config_dir",
    metavar="CONFIG_DIRECTORY",
    help="Path to a directory containing configuration files (e.g. /etc/my_app/config)",
)
parser.add_argument(
    "-v",
    "--verbose",
    action="store_true",
    help="Verbose mode - print parsed configuration",
    default=False,
)

special_model_cases = {
    (EntitySpecDict, "attribs"): AttrSpec,
}


def stringify_source(source) -> str:
    if isinstance(source, dict):
        source_str: str = yaml.dump(source, default_flow_style=False, width=1000)
        source_str = "```\n  " + source_str.replace("\n", "\n  ")[:-2] + "```"
    else:
        source_str = f"\t`{source}`"
    return source_str


def handle_attribs_error(data: dict, error: dict):
    locations = locate_attribs_error(data, error["msg"])
    path = list(error["loc"])

    for attr_name, source_data in locations:
        if len(path) > 3:
            loc_path = path[:3] + [attr_name] + path[3:]
            source = stringify_source(source_data)
        else:
            loc_path = path + [attr_name]
            source = stringify_source(source_data)

        print(" -> ".join(loc_path))
        print(source)

    print(f'{error["msg"]} (type={error["type"]})\n')


def locate_attribs_error(data: dict, sought_err: str) -> list[tuple[str, dict]]:
    locations = []

    for attr, attr_spec in data.items():
        try:
            AttrSpec(attr, attr_spec)
        except ValidationError as exception:
            for err_dict in exception.errors():
                if err_dict["msg"] == sought_err:
                    locations.append((attr, attr_spec))
        except ValueError as exception:
            if exception.args[0] == sought_err:
                locations.append((attr, attr_spec))

    return locations


def locate_basemodel_error(data: list[dict], model: BaseModel) -> list[dict]:
    locations = []
    if isinstance(data, list):
        for item in data:
            try:
                model(item)
            except (ValidationError, ValueError):
                locations.append(item)
    return locations


def get_source_or_handle_error_path(data: dict, error: dict) -> Optional[list]:
    """Locate source of an error in validated data using Pydantic error path."""
    err_path = error["loc"]

    # Kickstart the model exploration
    curr_model_dict = get_type_hints(ModelSpec)["config"]
    curr_model_origin = get_origin(curr_model_dict)
    curr_model = get_args(curr_model_dict)[1]

    for key in err_path[1:]:
        if curr_model_origin != dict and curr_model_origin is not None:
            return data

        if key in data:
            data = data[key]

            if (curr_model, key) in special_model_cases:
                curr_model = special_model_cases[curr_model, key]
                curr_model_dict = get_type_hints(curr_model)
                curr_model_origin = get_origin(curr_model_dict)

                if curr_model == AttrSpec:
                    return handle_attribs_error(data, error)
                continue

            if isinstance(curr_model_dict, dict):
                curr_model = curr_model_dict[key]

            curr_model_dict = get_type_hints(curr_model)
            curr_model_origin = get_origin(curr_model_dict)
            if curr_model_origin == dict:
                curr_model = get_args(curr_model_dict)[1]
        else:
            return data

    if curr_model == AttrSpec:
        return handle_attribs_error(data, error)
    return locate_basemodel_error(data, curr_model)


def locate_errors(exc: ValidationError, data: dict):
    """Locate errors in a ValidationError object"""
    paths = []
    sources = []
    errors = []

    for error in exc.errors():
        path = error["loc"]
        message = f'{error["msg"]} (type={error["type"]})'
        source = get_source_or_handle_error_path(data, error)
        if source:
            sources.append(source)
            paths.append(path)
            errors.append(message)

    return paths, sources, errors


def main():
    args = parser.parse_args()

    try:
        config = read_config_dir(args.config_dir, recursive=True)
    except OSError:
        print(f"Cannot open: {args.config_dir}", sys.stderr)
        sys.exit(1)

    assert "db_entities" in config, "Config for 'db_entities' is missing"
    assert "database" in config, "Config for 'database' is missing"

    db_config = config.get("db_entities")
    try:
        ModelSpec(db_config)
    except ValidationError as exc:
        print("Invalid model specification (check entity config):")
        paths, sources, errors = locate_errors(exc, db_config)
        for path, source, err in zip(paths, sources, errors):
            print(" -> ".join(path))
            print(stringify_source(source))
            print(err, "\n")
        sys.exit(1)

    if args.verbose:
        # Print parsed config as JSON (print unserializable objects using str())
        print(json.dumps(config, indent=4, default=str))
    sys.exit(0)


if __name__ == "__main__":
    main()
