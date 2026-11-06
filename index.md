# Dynamic Profile Processing Platform (DP³)

DP³ is a platform for maintaining information about entities that vary over time. It stores attributes of entities such as IP addresses, devices, users, or other identifiers, accepts incoming datapoints over time, and lets application-specific logic derive additional results.

DP³ provides the processing core, storage model, API, worker runtime, and the operational machinery around them. It handles the repetitive backend work such as database schema updates, state persistence, and parallel execution across multiple workers.

For application developers, the main building blocks are the **entity model** and **secondary modules**. You define entities and their attributes, then attach your own logic to the platform using hooks inserted into DP³'s existing pipelines. That lets you build application-specific behavior on top of DP³ primitives, while DP³ takes care of the underlying execution and storage concerns.

## Start here

If you are new to DP³, start with the task you want to accomplish:

- **Build a new local DP³ application** → [Get started with local DP³ app development](https://cesnet.github.io/dp3/howto/get-started/index.md)
- **Add a new attribute to your data model** → [How to add an attribute](https://cesnet.github.io/dp3/howto/add-attribute/index.md)
- **Connect an external producer** → [How to add an input module](https://cesnet.github.io/dp3/howto/add-input/index.md)
- **Add worker-side logic reacting to data** → [How to add a secondary module](https://cesnet.github.io/dp3/howto/add-module/index.md)
- **Deploy an application** → [How to deploy a DP³ application](https://cesnet.github.io/dp3/howto/deploy-app/index.md)
- **Work on DP³ itself** → [How to set up for DP³ platform development](https://cesnet.github.io/dp3/howto/develop-dp3/index.md)

The [How-to guides](https://cesnet.github.io/dp3/howto/index.md) are the best starting point when you know what you want to do. They link out to the concept and reference pages when you need more detail.

## Core concepts

As you start building an application, some DP³ concepts become important. You do not need to read all of them up front; use them when you need a clearer mental model:

- [Architecture](https://cesnet.github.io/dp3/architecture/index.md) - how the API, workers, database, queues, and modules fit together
- [Data model](https://cesnet.github.io/dp3/data_model/index.md) - entities, attributes, datapoints, and links
- [History management](https://cesnet.github.io/dp3/history_management/index.md) - validity intervals, current value, aggregation, and time-based behavior
- [Modules](https://cesnet.github.io/dp3/modules/index.md) - how secondary modules extend DP³
- [Hooks](https://cesnet.github.io/dp3/hooks/index.md) - when module callbacks run and what data is available to them

A practical reading order for new application developers is usually: **Get started** → **Add an attribute / add a module** → return to **Data model**, **History management**, **Modules**, and **Hooks** when questions come up.

## Reference docs for exact details

When you already know the concept or task and need exact syntax, switch to the reference pages:

- [API](https://cesnet.github.io/dp3/api/index.md) - HTTP endpoints, request formats, and responses
- [Configuration](https://cesnet.github.io/dp3/configuration/index.md) - file-by-file configuration reference
- [Code Reference](https://cesnet.github.io/dp3/reference/index.md) - generated Python API reference for DP³ internals

## Repository structure

- `dp3` - Python package containing the processing core and API
- `config` - example/default configuration
- `install` - deployment configuration
