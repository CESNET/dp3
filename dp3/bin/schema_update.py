"""
Update the database schema after making conflicting changes to the model.

Authors: Ondřej Sedláček <xsedla1o@stud.fit.vutbr.cz>
"""

import logging

from dp3.common.config import ModelSpec, read_config_dir
from dp3.database.database import EntityDatabase


def init_parser(parser):
    parser.add_argument(
        "config",
        metavar="CONFIG_DIR",
        help="Path to a directory containing configuration files (e.g. /etc/my_app/config)",
    )
    parser.add_argument(
        "--bypass",
        action="store_true",
        default=False,
        help="Bypass the suggested database changes and update the schema regardless.",
    )


def confirm_changes(prompt: str):
    while True:
        answer = input(prompt).lower()
        if answer == "" or answer[0] == "n":
            return False
        if answer[0] == "y":
            return True


def main(args):
    # Load DP3 configuration
    config = read_config_dir(args.config, recursive=True)

    # Setup logging
    LOGFORMAT = "%(asctime)-15s,%(name)s,[%(levelname)s] %(message)s"
    LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(level=logging.DEBUG, format=LOGFORMAT, datefmt=LOGDATEFORMAT)
    log = logging.getLogger("SchemaUpdate")

    # Connect to database
    db = EntityDatabase(
        config,
        ModelSpec(config.get("db_entities")),
        config.get("processing_core.worker_processes"),
    )

    prev_schema, config_schema, updates, deleted_entites = db.schema_cleaner.get_schema_status()
    if prev_schema["version"] != config_schema["version"]:
        log.info(
            f"Schema version mismatch: {prev_schema['version']} (DB) "
            f"!= {config_schema['version']} (config)"
        )
        if confirm_changes("Are you sure you want to perform a migration now? (y/[n]): "):
            db.schema_cleaner.migrate(prev_schema)
        return

    if prev_schema["storage"] != config_schema["storage"]:
        log.info(
            f"Storage mismatch: {prev_schema['storage']} (DB) "
            f"!= {config_schema['storage']} (config)"
        )
        if confirm_changes("Are you sure you want to change the storage now? (y/[n]): "):
            db.schema_cleaner.update_storage(prev_schema["storage"], config_schema["storage"])
    elif prev_schema["schema"] == config_schema["schema"]:
        log.info("Schema is OK!")
        return

    if not updates and not deleted_entites:
        db.schema_cleaner.schemas.insert_one(config_schema)
        log.info("Updated schema without any changes to master records, OK now!")
        return

    if deleted_entites:
        log.info("Suggested removal of entities:")
        for entity in deleted_entites:
            log.info(f"- {entity}")

    if updates:
        log.info("Suggested changes to master records:")
        for entity, entity_updates in updates.items():
            log.info(f"- {entity}: {dict(entity_updates)}")

    if args.bypass:
        if not confirm_changes(
            "Are you sure you want update the schema without applying changes? (y/[n]): "
        ):
            log.info("Aborted schema update.")
            return
        db.schema_cleaner.schemas.insert_one(config_schema)
        log.info("Bypassing suggested changes, updated schema, OK now!")
        return

    if not confirm_changes("Are you sure you want to apply these changes? (y/[n]): "):
        log.info("Aborted schema update.")
        return
    db.schema_cleaner.execute_updates(updates, deleted_entites)
    db.schema_cleaner.schemas.insert_one(config_schema)
    log.info("Applied suggested changes, updated schema, OK now!")
