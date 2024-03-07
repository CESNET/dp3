# Configuration

DP³ configuration folder consists of these files and folders:

- [`📁 db_entities`](db_entities.md) - Database entities configuration folder. This is your data model.
- [`📁 modules`](modules.md) - Modules configuration folder.
- [`📄 api.yml`](api.md) - API configuration file.
- [`📄 control.yml`](control.md) - Configuration file controlling allowed `/control` endpoint actions.
- [`📄 database.yml`](database.md) - Connection to the DB.
- [`📄 event_logging.yml`](event_logging.md) - Tracking the app operation using Redis.
- [`📄 garbage_collector.yml`](garbage_collector.md) - Removing entities with expired [lifetimes](lifetimes.md).
- [`📄 history_manager.yml`](history_manager.md) - How often is [history management](../history_management.md) performed.
- [`📄 processing_core.yml`](processing_core.md) - Settings of main application workers.
- [`📄 snapshots.yml`](snapshots.md) - How often are entity snapshots taken.
- [`📄 updater.yml`](updater.md) - Periodic updates of all entities over a longer time frame.

The details of their meaning and usage is explained in their relative pages.

## Example configuration

Example configuration is included `config/` folder in [DP³ repository](https://github.com/CESNET/dp3/).
