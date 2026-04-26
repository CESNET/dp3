# Configuration

DP³ configuration is split across multiple files. You can browse them by filename below, but if you need to configure a specific aspect of the platform, start with these task-oriented groups. For step-by-step workflows, see the [How-to guides](../howto/).

## How to configure the platform and required services

Use these pages when wiring DP³ to MongoDB, RabbitMQ, Redis, and the basic API/runtime settings needed to get the platform running. The supplied Docker Compose setup already starts MongoDB, RabbitMQ, and Redis for local development, but their connection details and runtime knobs are still configured in these files.

- [`📄 database.yml`](database/) - MongoDB connection and storage settings.
- [`📄 processing_core.yml`](processing_core/) - Worker processes, threads, RabbitMQ connection, and enabled modules.
- [`📄 event_logging.yml`](event_logging/) - Redis-based event logging.
- [`📄 api.yml`](api/) - API-facing settings, including datapoint logging.
- [`📄 control.yml`](control/) - Operational actions exposed by the `/control` endpoint.

## How to configure the data model and entity lifecycle

Use these pages to define entities and attributes, decide how long entities should live, and understand how cleanup settings affect them. Entities are immortal by default, so most applications should review lifetime and cleanup settings explicitly.

- [`📁 db_entities`](db_entities/) - Entity and attribute definitions.
- [`📄 lifetimes`](lifetimes/) - Lifetime policies such as immortal, TTL, and weak entities.
- [`📄 garbage_collector.yml`](garbage_collector/) - How often expired entities are collected.
- [`📄 history_manager.yml`](history_manager/) - Cleanup and retention tasks that affect stored history and snapshots.
- [How to add an attribute](../howto/add-attribute/) - End-to-end rollout of a newly configured attribute.

## How to configure snapshots

Snapshot behavior spans several files: whether an entity supports snapshots, when snapshots are created, how long they are kept, how they are stored, and whether they can be triggered manually.

- [`📄 snapshots.yml`](snapshots/) - Snapshot creation schedule and snapshot-specific options.
- [`📁 db_entities`](db_entities/) - Per-entity snapshot enablement via `entity.snapshot`.
- [`📄 history_manager.yml`](history_manager/) - Snapshot cleanup schedule and retention window.
- [`📄 database.yml`](database/) - Snapshot storage-related database settings.
- [`📄 control.yml`](control/) - Manual snapshot triggering through `make_snapshots`.

## How to configure data retention, cleanup, and archival

Use these pages when deciding how long datapoints and snapshots are kept, when old data is aggregated or deleted, and whether raw datapoints are archived.

- [`📁 db_entities`](db_entities/) - Per-attribute retention settings such as `history_params.max_age` and `timeseries_params.max_age`.
- [`📄 history_manager.yml`](history_manager/) - Aggregation, cleaning, snapshot pruning, and datapoint archivation schedules.
- [`📄 lifetimes`](lifetimes/) - How incoming data can extend entity lifetime.
- [`📄 garbage_collector.yml`](garbage_collector/) - Final collection of expired entities.

## How to configure modules, new attributes, and periodic jobs

Use these pages when enabling modules, adding module-specific configuration, scheduling periodic updates, or rolling out a newly emitted attribute. If a module or external producer starts sending a new attribute, define it in [`db_entities`](db_entities/) first, validate the configuration with `dp3 check`, and then reload the affected API and worker processes.

- [`📄 processing_core.yml`](processing_core/) - Module loading, worker runtime, and enabled modules.
- [`📁 modules`](modules/) - Module-specific configuration files.
- [`📄 updater.yml`](updater/) - Periodic batch updates over stored entities.
- [`📄 control.yml`](control/) - Refresh actions for module configuration and entity initialization.
- [`📁 db_entities`](db_entities/) - Attribute definitions required for module-produced data.
- [How to add an attribute](../howto/add-attribute/) - The shared workflow for attributes emitted by external producers and DP³ modules.
- [How to add an input module](../howto/add-input/) - Connect an external producer to the DP³ API.
- [How to add a secondary module](../howto/add-module/) - Add worker-side logic that reacts to incoming or stored data.

## Reference by file

DP³ configuration folder consists of these files and folders:

- [`📁 db_entities`](db_entities/) - Database entities configuration folder. This is your data model.
- [`📁 modules`](modules/) - Modules configuration folder.
- [`📄 api.yml`](api/) - API configuration file.
- [`📄 control.yml`](control/) - Configuration file controlling allowed `/control` endpoint actions.
- [`📄 database.yml`](database/) - Connection to the DB.
- [`📄 event_logging.yml`](event_logging/) - Tracking the app operation using Redis.
- [`📄 garbage_collector.yml`](garbage_collector/) - Removing entities with expired [lifetimes](lifetimes/).
- [`📄 history_manager.yml`](history_manager/) - How often is [history management](../history_management/) performed.
- [`📄 processing_core.yml`](processing_core/) - Settings of main application workers.
- [`📄 snapshots.yml`](snapshots/) - How often are entity snapshots taken.
- [`📄 updater.yml`](updater/) - Periodic updates of all entities over a longer time frame.

The details of their meaning and usage are explained on their respective pages.

## Example configuration

Example configuration is included `config/` folder in [DP³ repository](https://github.com/CESNET/dp3/).
