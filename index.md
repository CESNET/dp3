# Dynamic Profile Processing Platform (DP³)

DP³ is a platform for maintaining information about entities that vary over time. It stores attributes of entities such as IP addresses, devices, users, or other identifiers, accepts incoming datapoints over time, and lets application-specific logic derive additional results.

DP³ provides the processing core, storage model, API, worker runtime, and the operational machinery around them. It handles the repetitive backend work such as database schema updates, state persistence, and parallel execution across multiple workers.

For application developers, the main building blocks are the **entity model** and **secondary modules**. You define entities and their attributes, then attach your own logic to the platform using hooks inserted into DP³'s existing pipelines. That lets you build application-specific behavior on top of DP³ primitives, while DP³ takes care of the underlying execution and storage concerns.

## Start here

If you are new to DP³, start with the task you want to accomplish:

- **Build a new local DP³ application** → [Get started with local DP³ app development](howto/get-started/)
- **Add a new attribute to your data model** → [How to add an attribute](howto/add-attribute/)
- **Connect an external producer** → [How to add an input module](howto/add-input/)
- **Add worker-side logic reacting to data** → [How to add a secondary module](howto/add-module/)
- **Deploy an application** → [How to deploy a DP³ application](howto/deploy-app/)
- **Work on DP³ itself** → [How to set up for DP³ platform development](howto/develop-dp3/)

The [How-to guides](howto/) are the best starting point when you know what you want to do. They link out to the concept and reference pages when you need more detail.

## Core concepts

As you start building an application, some DP³ concepts become important. You do not need to read all of them up front; use them when you need a clearer mental model:

- [Architecture](architecture/) - how the API, workers, database, queues, and modules fit together
- [Data model](data_model/) - entities, attributes, datapoints, and links
- [History management](history_management/) - validity intervals, current value, aggregation, and time-based behavior
- [Modules](modules/) - how secondary modules extend DP³
- [Hooks](hooks/) - when module callbacks run and what data is available to them

A practical reading order for new application developers is usually: **Get started** → **Add an attribute / add a module** → return to **Data model**, **History management**, **Modules**, and **Hooks** when questions come up.

## Reference docs for exact details

When you already know the concept or task and need exact syntax, switch to the reference pages:

- [API](api/) - HTTP endpoints, request formats, and responses
- [Configuration](configuration/) - file-by-file configuration reference
- [Code Reference](reference/) - generated Python API reference for DP³ internals

## Repository structure

- `dp3` - Python package containing the processing core and API
- `config` - example/default configuration
- `install` - deployment configuration
