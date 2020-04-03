#!/usr/bin/env python3
"""Script to put a single task (aka update_request) to the main Task Queue."""

import sys
import argparse
import logging
import json

import os
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')))
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))

from task_queue import TaskQueueWriter
from src.common.config import read_config_dir

LOGFORMAT = "%(asctime)-15s,%(name)s [%(levelname)s] %(message)s"
LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)

logger = logging.getLogger('PutTask')

# parse arguments
parser = argparse.ArgumentParser(
    prog="put_task.py",
    description="Put a single task (aka update_request) to the main Task Queue."
)
parser.add_argument('-c', '--config', metavar='FILENAME', default='../../config',
                    help='Path to configuration dir (default: ../config)')
parser.add_argument('-tf' '--taskfile', dest="task", metavar='FILENAME', default='new_task.json',
                    help="Path to file, where the body of the task is saved")
parser.add_argument("-v", dest="verbose", action="store_true", help="Verbose mode")
args = parser.parse_args()

if args.verbose:
    logger.setLevel("DEBUG")

# Load configuration
logger.debug(f"Loading config dir {args.config}")
config = read_config_dir(args.config)

rabbit_params = config.get('rabbitmq', {})
num_processes = config.get('worker_processes')

try:
    with open(args.task) as task_file:
        task_body = json.load(task_file)
except FileNotFoundError:
    logger.error("Specified input file with task does not exist")
    sys.exit(1)
except ValueError:
    logger.error("Task is not in correct JSON format")
    sys.exit(1)

# Create connection to task queue (RabbitMQ)
tqw = TaskQueueWriter(num_processes, rabbit_params)
if args.verbose:
    tqw.log.setLevel("DEBUG")
tqw.connect()

# Put task
logger.debug(f"Sending task for {task_body['etype']}/{task_body['ekey']}: \n"
             f"attr_updates: {task_body['attr_updates']}\n"
             f"data_points: {task_body['data_points']}")
tqw.put_task(**task_body)

# Close connection
tqw.disconnect()
