#!/usr/bin/env python3
"""Converts legacy CSV DataPoint log format to JSON"""
import argparse
import gzip
import ipaddress
import json
import logging
import os
import re
from typing import Any, Callable

import pandas as pd
from dateutil.parser import parse as parsetime
from pydantic import ValidationError

from dp3.common.config import ModelSpec, read_config_dir

re_timestamp = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?([Zz]|(?:[+-]\d{2}:\d{2}))?$"
)
re_mac = re.compile(r"^([0-9a-fA-F]{2}[:-]){5}([0-9a-fA-F]{2})$")
re_array = re.compile(r"^array<(\w+)>$")
re_set = re.compile(r"^set<(\w+)>$")
re_link = re.compile(r"^link<(\w+)>$")
re_dict = re.compile(r"^dict<((\w+\??:\w+,)*(\w+\??:\w+))>$")
re_category = re.compile(
    r"^category<\s*(?P<type>\w+)\s*;\s*(?P<vals>(?:\s*\w+,\s*)*(?:\s*\w+\s*))>$"
)


def valid_ipv4(address):
    try:
        ipaddress.IPv4Address(address)
        return True
    except ValueError:
        return False


def valid_ipv6(address):
    try:
        ipaddress.IPv6Address(address)
        return True
    except ValueError:
        return False


# Validate MAC string
def valid_mac(address):
    return re_mac.match(address)


logging.basicConfig(level=logging.INFO, format="%(name)s [%(levelname)s] %(message)s")

# Dictionary containing conversion functions for primitive data types
CONVERTERS = {
    "tag": lambda v: json.loads(f'{{"v": {v}}}')["v"],
    "binary": lambda v: v.lower() == "true",
    "string": str,
    "int": int,
    "int64": int,
    "float": float,
    "ipv4": lambda v: _pass_valid(valid_ipv4, v),
    "ipv6": lambda v: _pass_valid(valid_ipv6, v),
    "mac": lambda v: _pass_valid(valid_mac, v),
    "time": parsetime,
    "json": json.loads,
}


def get_converter(attr_data_type: str) -> Callable[[str], Any]:
    """Return a function converting a string to given data type."""
    # basic type
    if attr_data_type in CONVERTERS:
        return CONVERTERS[attr_data_type]
    # array<X>, set<X>, dict<X,Y,Z>
    if (
        re.match(re_array, attr_data_type)
        or re.match(re_set, attr_data_type)
        or re.match(re_dict, attr_data_type)
    ):
        return json.loads
    # link<X>
    if re.match(re_link, attr_data_type):
        return str
    # category<X; Y>
    if re.match(re_category, attr_data_type):
        return str
    raise ValueError(f"No conversion function for attribute type '{attr_data_type}'")


def _parse_array_str(string: str, item_type: str) -> list:
    """Parse string containing array of items of given type"""
    conv = CONVERTERS[item_type]
    a = json.loads(string)
    if not isinstance(a, list):
        raise ValueError
    return [conv(item) for item in a]


def _parse_set_str(string: str, item_type: str) -> set:
    conv = CONVERTERS[item_type]
    a = json.loads(string)
    if not isinstance(a, list):
        raise ValueError
    return {conv(item) for item in a}


def _parse_dict_str(string: str, field_types: dict) -> dict:
    o = json.loads(string)
    if not isinstance(o, dict):
        raise ValueError
    return {k: CONVERTERS[field_types[k]](v) for k, v in o.items()}


def _pass_valid(validator_function, value):
    if validator_function(value):
        return value
    raise ValueError(f"The value {value} has invalid format.")


class LegacyDataPointLoader:
    """Loader of datapoint files as written by DP3 API receiver."""

    # Names of columns in datapoint files
    COL_NAMES = ["type", "id", "attr", "t1", "t2", "c", "src", "v"]
    log = logging.getLogger("LegacyDataPointLoader")

    def __init__(self, attr_config_dirname: str):
        """
        Create a datapoint loader.

        attr_config_dirname: Directory with attribute configuration (same as for DP3)
        """
        # Load attribute config
        model_spec = ModelSpec(read_config_dir(attr_config_dirname))

        # Prepare a table for data type conversion
        # (to get data type from model_spec: model_spec[etype]["attribs"][attrname].data_type)
        self.dt_conv = {}  # map (etype,attr_name) -> conversion_function
        for etype, spec in model_spec.items():
            for aname, aspec in spec["attribs"].items():
                data_type = getattr(aspec, "data_type", None)
                converter = json.loads if data_type is None else get_converter(str(data_type))
                self.dt_conv[(etype, aname)] = converter

        self.model_spec = model_spec

    def read_dp_file(self, filename: str) -> pd.DataFrame:
        """
        Read a file with ADiCT/DP3 datapoints into pandas DataFrame.

        Values of attributes in datapoints are validated and converted according
        to the attribute configuration passed to LegacyDataPointLoader constructor.
        """
        open_function = gzip.open if filename.endswith(".gz") else open

        # Reformat datapoints file so "val" containing commas can be read properly.
        #   Replace first 7 commas (i.e. all except those inside "v") with semicolon
        #   Store as temporary file
        tmp_name = (
            f"tmp-{'.'.join(os.path.basename(os.path.normpath(filename)).split(sep='.')[:-1])}"
        )
        with open_function(filename, "rb") as infile, open(tmp_name, "wb") as outfile:
            for line in infile:
                outfile.write(line.replace(b",", b";", 7))
        # Load the converted file
        data = pd.read_csv(
            tmp_name,
            sep=";",
            header=None,
            names=self.COL_NAMES,
            index_col=False,
            converters={"c": float, "v": str},
            escapechar="\\",
            # parse_dates=["t1", "t2"],
            # infer_datetime_format=True,
        )
        # Cleanup
        if os.path.exists(tmp_name):
            os.remove(tmp_name)

        # Convert values to correct types according to model_spec
        def convert_row(row):
            try:
                row[2] = self.dt_conv[(row[0], row[1])](row[2])
            except KeyError as e:
                raise KeyError(f"No converter for {(row[0], row[1])}, with value {row[2]}.") from e
            except ValueError:
                self.log.error("ValueError in conversion, v: %s", row)
                return row
            return row

        attrs = {entity_attr[1] for entity_attr in self.dt_conv}
        conv_vals = data.loc[data["attr"].isin(attrs), ("type", "attr", "v")].apply(
            convert_row, axis=1, raw=True
        )
        if len(conv_vals) != len(data):
            self.log.warning(
                "Dropped %s rows due to missing attributes in config", len(data) - len(conv_vals)
            )
            self.log.info("Missing attrs: %s", [x for x in data["attr"].unique() if x not in attrs])
        data["v"] = conv_vals["v"]
        return data[data["attr"].apply(lambda x: x in attrs)]


def get_valid_path(parser, arg):
    if not os.path.exists(arg):
        parser.error(f"The file {arg} does not exist!")
    else:
        return os.path.abspath(arg)


def get_out_path(in_file_path, output_dir):
    """
    Return output file path based on the input file.
    Parses the date from input filename, fits date into prepared pattern: "dp_log_{date}.json".
    """
    date = in_file_path.split("-")[-1]
    if date.endswith(".gz"):
        date = date[:-3]
    out_filename = f"dp_log_{date}.json"
    return os.path.join(output_dir, out_filename)


def validate_row(row):
    # COL_NAMES = ["type", "id", "attr", "t1", "t2", "c", "src", "v"]

    dp_obj = {
        "etype": row[0],
        "eid": row[1],
        "attr": row[2],
        "t1": row[3],
        "t2": row[4],
        "c": row[5],
        "src": row[6],
        "v": row[7],
    }

    etype = dp_obj["etype"]
    attr = dp_obj["attr"]

    try:
        model_spec.attr(etype, attr).dp_model.model_validate(dp_obj)
    except ValidationError as err:
        print(model_spec.attr(etype, attr))
        print(attr, type(dp_obj["v"]), repr(dp_obj["v"]), dp_obj["t1"], dp_obj["t2"])
        raise err


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converts legacy CSV DataPoint log format to JSON")
    parser.add_argument(
        "-c",
        "--attr_conf_dir",
        dest="attr_conf_dir",
        required=True,
        help="Path to DP3 entity config",
        type=lambda x: get_valid_path(parser, x),
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        dest="output_dir",
        default=os.path.curdir,
        help="Converted files will be saved to this path",
        type=lambda x: get_valid_path(parser, x),
    )
    parser.add_argument(
        "files", help="Legacy CSV file paths", type=lambda x: get_valid_path(parser, x), nargs="+"
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        default=False,
        help="Compress output file using gzip (.gz)",
    )
    args = parser.parse_args()

    loader = LegacyDataPointLoader(args.attr_conf_dir)
    model_spec = loader.model_spec

    for filename in args.files:
        dp_log = loader.read_dp_file(filename)

        dp_log.apply(validate_row, axis=1, raw=True)

        converted_filename = get_out_path(filename, args.output_dir)
        converted_filename = converted_filename + ".gz" if args.compress else converted_filename
        dp_log.to_json(converted_filename, orient="records", indent=1)
