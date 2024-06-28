"""
Load and check configuration from given directory, print any errors, and exit.

TODO:
 - refactor to simplify the code, some error path matching must be done to counteract
   the AttrSpec function magic where Pydantic fails, but otherwise it is not required
"""

import argparse
import enum
import json
import sys
from datetime import datetime
from json import JSONEncoder
from typing import Any, get_args, get_origin, get_type_hints

import yaml
from pydantic import BaseModel, ValidationError

from dp3.common.attrspec import AttrSpec
from dp3.common.config import EntitySpecDict, ModelSpec, read_config_dir

special_model_cases = {
    (EntitySpecDict, "attribs"): AttrSpec,
}


class ConfigEncoder(JSONEncoder):
    """JSONEncoder to encode parsed configuration."""

    def default(self, o: Any) -> Any:
        if isinstance(o, BaseModel):
            return o.model_dump(exclude_none=True)
        if isinstance(o, enum.Enum):
            return o.value
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        return str(o)


def stringify_source(source) -> str:
    if source is None:
        return ""

    if isinstance(source, dict):
        source_str: str = yaml.dump(source, default_flow_style=False, width=1000)
        source_str = "```\n  " + source_str.replace("\n", "\n  ")[:-2] + "```"
    else:
        source_str = f"\t`{source}`"
    return source_str


def get_all_attribs_errors(data: dict, error: dict) -> tuple[list[tuple[str]], list]:
    paths, sources = locate_attribs_error(data, error["msg"])

    path: list[str] = list(error["loc"])
    paths = [tuple(path[:3] + list(p)) for p in paths]

    return paths, sources


def locate_attribs_error(data: dict, sought_err: str) -> tuple[list[tuple], list[dict]]:
    """
    Locate source of an error in a dict of AttrSpecs.
    Returns all sources of the same kind of error.
    """
    paths = []
    sources = []

    for attr, attr_spec in data.items():
        try:
            AttrSpec(attr, attr_spec)
        except ValidationError as exception:
            for err_dict in exception.errors():
                if err_dict["msg"] == sought_err:
                    paths.append((attr, *err_dict["loc"]))
                    sources.append(attr_spec)
        except (ValueError, AssertionError) as exception:
            if sought_err.endswith(exception.args[0]):
                paths.append((attr,))
                sources.append(attr_spec)

    return paths, sources


def locate_basemodel_error(data: list[dict], model: BaseModel) -> list[dict]:
    locations = []
    if isinstance(data, list):
        for item in data:
            try:
                model(item)
            except (ValidationError, ValueError, AssertionError):
                locations.append(item)
    return locations


def get_error_sources(data: dict, error: dict) -> tuple[list, list]:
    """Locate source of an error in validated data using Pydantic error path."""
    err_path = error["loc"]

    # Kickstart the model exploration
    curr_model_dict = get_type_hints(ModelSpec)["config"]
    curr_model_origin = get_origin(curr_model_dict)
    curr_model = get_args(curr_model_dict)[1]

    for key in err_path[1:]:
        if curr_model_origin is not dict and curr_model_origin is not None:
            return [err_path], [data]

        if key in data:
            prev_data = data
            data = data[key]

            if (curr_model, key) in special_model_cases:
                curr_model = special_model_cases[curr_model, key]
                curr_model_dict = get_type_hints(curr_model)
                curr_model_origin = get_origin(curr_model_dict)

                if curr_model == AttrSpec:
                    return get_all_attribs_errors(data, error)
                continue

            if isinstance(curr_model_dict, dict):
                if key in curr_model_dict:
                    curr_model = curr_model_dict[key]
                else:
                    return [err_path], [prev_data]

            curr_model_dict = get_type_hints(curr_model)
            curr_model_origin = get_origin(curr_model_dict)
            if curr_model_origin is dict:
                curr_model = get_args(curr_model_dict)[1]
        else:
            return [err_path], [data]

    if curr_model == AttrSpec:
        return get_all_attribs_errors(data, error)

    return [], locate_basemodel_error(data, curr_model)


def locate_errors(exc: ValidationError, data: dict):
    """Locate errors (i.e.: get the paths and sources) in a ValidationError object."""
    paths = []
    sources = []
    errors = []

    for error in exc.errors():
        if error["loc"] == ():
            paths.append(())
            sources.append(None)
            errors.append(error["msg"])
            continue

        message = f'{error["msg"]} (type={error["type"]})'
        e_paths, e_sources = get_error_sources(data, error)

        paths.extend(e_paths)
        sources.extend(e_sources)
        errors.extend(message for _ in range(len(e_paths)))

    return paths, sources, errors


def init_parser(parser):
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


def run():
    print(
        "WARNING: The `check` entrypoint is deprecated due to possible namespace conflicts. "
        "Please use `dp3 check` instead.",
        file=sys.stderr,
    )

    # Parse arguments
    parser = argparse.ArgumentParser(
        prog="check",
        description="Load configuration from given directory and check its validity. "
        "When configuration is OK, program exits immediately with status code 0, "
        "otherwise it prints error messages on stderr and exits with non-zero status.",
    )
    init_parser(parser)
    args = parser.parse_args()

    main(args)


def main(args):
    try:
        config = read_config_dir(args.config_dir, recursive=True)
    except OSError:
        print(f"Cannot open: {args.config_dir}", sys.stderr)
        sys.exit(1)

    assert "db_entities" in config, "Config for 'db_entities' is missing"
    assert "database" in config, "Config for 'database' is missing"

    db_config = config.get("db_entities")
    try:
        parsed_config = ModelSpec(db_config)
    except ValidationError as exc:
        print("Invalid model specification (check entity config):")
        paths, sources, errors = locate_errors(exc, db_config)

        # Print each source only once
        unique_sources = []
        source_paths_and_errors = []

        for path, source, err in zip(paths, sources, errors):
            if source in unique_sources:
                i = unique_sources.index(source)
                source_paths_and_errors[i].add((path, err))
            else:
                unique_sources.append(source)
                source_paths_and_errors.append({(path, err)})

        for source, paths_and_errors in zip(unique_sources, source_paths_and_errors):
            for path, err in paths_and_errors:
                print(" -> ".join(path))
                print("  ", err)
            print(stringify_source(source), "\n")
        sys.exit(1)

    if args.verbose:
        # Print parsed config as JSON (print unserializable objects using str())
        print(json.dumps(parsed_config.config, indent=4, cls=ConfigEncoder))
    sys.exit(0)


if __name__ == "__main__":
    run()
