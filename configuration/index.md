# Configuration

DP³ configuration is split across multiple files. You can browse them by filename below, but if you need to configure a specific aspect of the platform, start with these task-oriented groups. For step-by-step workflows, see the [How-to guides](https://cesnet.github.io/dp3/howto/index.md).

## How to configure the platform and required services

Use these pages when wiring DP³ to MongoDB, RabbitMQ, Redis, and the basic API/runtime settings needed to get the platform running. The supplied Docker Compose setup already starts MongoDB, RabbitMQ, and Redis for local development, but their connection details and runtime knobs are still configured in these files.

- [`📄 database.yml`](https://cesnet.github.io/dp3/configuration/database/index.md) - MongoDB connection and storage settings.
- [`📄 processing_core.yml`](https://cesnet.github.io/dp3/configuration/processing_core/index.md) - Worker processes, threads, RabbitMQ connection, and enabled modules.
- [`📄 event_logging.yml`](https://cesnet.github.io/dp3/configuration/event_logging/index.md) - Redis-based event logging.
- [`📄 api.yml`](https://cesnet.github.io/dp3/configuration/api/index.md) - API-facing settings, including datapoint logging.
- [`📄 control.yml`](https://cesnet.github.io/dp3/configuration/control/index.md) - Operational actions exposed by the `/control` endpoint.

## How to configure the data model and entity lifecycle

Use these pages to define entities and attributes, decide how long entities should live, and understand how cleanup settings affect them. Entities are immortal by default, so most applications should review lifetime and cleanup settings explicitly.

- [`📁 db_entities`](https://cesnet.github.io/dp3/configuration/db_entities/index.md) - Entity and attribute definitions.
- [`📄 lifetimes`](https://cesnet.github.io/dp3/configuration/lifetimes/index.md) - Lifetime policies such as immortal, TTL, and weak entities.
- [`📄 garbage_collector.yml`](https://cesnet.github.io/dp3/configuration/garbage_collector/index.md) - How often expired entities are collected.
- [`📄 history_manager.yml`](https://cesnet.github.io/dp3/configuration/history_manager/index.md) - Cleanup and retention tasks that affect stored history and snapshots.
- [How to add an attribute](https://cesnet.github.io/dp3/howto/add-attribute/index.md) - End-to-end rollout of a newly configured attribute.

## How to configure snapshots

Snapshot behavior spans several files: whether an entity supports snapshots, when snapshots are created, how long they are kept, how they are stored, and whether they can be triggered manually.

- [`📄 snapshots.yml`](https://cesnet.github.io/dp3/configuration/snapshots/index.md) - Snapshot creation schedule and snapshot-specific options.
- [`📁 db_entities`](https://cesnet.github.io/dp3/configuration/db_entities/index.md) - Per-entity snapshot enablement via `entity.snapshot`.
- [`📄 history_manager.yml`](https://cesnet.github.io/dp3/configuration/history_manager/index.md) - Snapshot cleanup schedule and retention window.
- [`📄 database.yml`](https://cesnet.github.io/dp3/configuration/database/index.md) - Snapshot storage-related database settings.
- [`📄 control.yml`](https://cesnet.github.io/dp3/configuration/control/index.md) - Manual snapshot triggering through `make_snapshots`.

## How to configure data retention, cleanup, and archival

Use these pages when deciding how long datapoints and snapshots are kept, when old data is aggregated or deleted, and whether raw datapoints are archived.

- [`📁 db_entities`](https://cesnet.github.io/dp3/configuration/db_entities/index.md) - Per-attribute retention settings such as `history_params.max_age` and `timeseries_params.max_age`.
- [`📄 history_manager.yml`](https://cesnet.github.io/dp3/configuration/history_manager/index.md) - Aggregation, cleaning, snapshot pruning, and datapoint archivation schedules.
- [`📄 lifetimes`](https://cesnet.github.io/dp3/configuration/lifetimes/index.md) - How incoming data can extend entity lifetime.
- [`📄 garbage_collector.yml`](https://cesnet.github.io/dp3/configuration/garbage_collector/index.md) - Final collection of expired entities.

## How to configure modules, new attributes, and periodic jobs

Use these pages when enabling modules, adding module-specific configuration, scheduling periodic updates, or rolling out a newly emitted attribute. If a module or external producer starts sending a new attribute, define it in [`db_entities`](https://cesnet.github.io/dp3/configuration/db_entities/index.md) first, validate the configuration with `dp3 check`, and then reload the affected API and worker processes.

- [`📄 processing_core.yml`](https://cesnet.github.io/dp3/configuration/processing_core/index.md) - Module loading, worker runtime, and enabled modules.
- [`📁 modules`](https://cesnet.github.io/dp3/configuration/modules/index.md) - Module-specific configuration files.
- [`📄 updater.yml`](https://cesnet.github.io/dp3/configuration/updater/index.md) - Periodic batch updates over stored entities.
- [`📄 control.yml`](https://cesnet.github.io/dp3/configuration/control/index.md) - Refresh actions for module configuration and entity initialization.
- [`📁 db_entities`](https://cesnet.github.io/dp3/configuration/db_entities/index.md) - Attribute definitions required for module-produced data.
- [How to add an attribute](https://cesnet.github.io/dp3/howto/add-attribute/index.md) - The shared workflow for attributes emitted by external producers and DP³ modules.
- [How to add an input module](https://cesnet.github.io/dp3/howto/add-input/index.md) - Connect an external producer to the DP³ API.
- [How to add a secondary module](https://cesnet.github.io/dp3/howto/add-module/index.md) - Add worker-side logic that reacts to incoming or stored data.

## Reference by file

DP³ configuration folder consists of these files and folders:

- [`📁 db_entities`](https://cesnet.github.io/dp3/configuration/db_entities/index.md) - Database entities configuration folder. This is your data model.
- [`📁 modules`](https://cesnet.github.io/dp3/configuration/modules/index.md) - Modules configuration folder.
- [`📄 api.yml`](https://cesnet.github.io/dp3/configuration/api/index.md) - API configuration file.
- [`📄 control.yml`](https://cesnet.github.io/dp3/configuration/control/index.md) - Configuration file controlling allowed `/control` endpoint actions.
- [`📄 database.yml`](https://cesnet.github.io/dp3/configuration/database/index.md) - Connection to the DB.
- [`📄 event_logging.yml`](https://cesnet.github.io/dp3/configuration/event_logging/index.md) - Tracking the app operation using Redis.
- [`📄 garbage_collector.yml`](https://cesnet.github.io/dp3/configuration/garbage_collector/index.md) - Removing entities with expired [lifetimes](https://cesnet.github.io/dp3/configuration/lifetimes/index.md).
- [`📄 history_manager.yml`](https://cesnet.github.io/dp3/configuration/history_manager/index.md) - How often is [history management](https://cesnet.github.io/dp3/history_management/index.md) performed.
- [`📄 processing_core.yml`](https://cesnet.github.io/dp3/configuration/processing_core/index.md) - Settings of main application workers.
- [`📄 snapshots.yml`](https://cesnet.github.io/dp3/configuration/snapshots/index.md) - How often are entity snapshots taken.
- [`📄 updater.yml`](https://cesnet.github.io/dp3/configuration/updater/index.md) - Periodic updates of all entities over a longer time frame.

The details of their meaning and usage are explained on their respective pages.

## Example configuration

Example configuration is included `config/` folder in [DP³ repository](https://github.com/CESNET/dp3/).
