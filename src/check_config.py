#!/usr/bin/env python3
"""
Load and check configuration from given directory, print any errors, and exit.
"""

import sys
import argparse
import json
from common.config import read_config_dir, load_attr_spec

# Parse arguments
parser = argparse.ArgumentParser(
    prog="check_config.py",
    description="Load configuration from given directory and check its validity. When configuration is OK, program "
                "exits immediately with status code 0, otherwise it prints error messages on stderr and exits with non-zero "
                "status."
)
parser.add_argument('config_dir', metavar='CONFIG_DIR', help='Path to configuration directory (e.g. /etc/adict/config)')
parser.add_argument('-v', '--verbose', action="store_true", help="Verbose mode - print parsed configuration", default=False)
args = parser.parse_args()

try:
    config = read_config_dir(args.config_dir, True)
    attr_spec = load_attr_spec(config["db_entities"])
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

if args.verbose:
    print(json.dumps(config, indent=4))
sys.exit(0)
