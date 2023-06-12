# Dynamic Profile Processing Platform (DP³)

DP³ is a platform helps to keep a database of information (attributes) about individual
entities (designed for IP addresses and other network identifiers, but may be anything),
when the data constantly changes in time.

DP³ doesn't do much by itself, it must be supplemented by application-specific modules providing
and processing data.

This is a basis of CESNET's "Asset Discovery Classification and Tagging" (ADiCT) project,
focused on discovery and classification of network devices,
but the platform itself is general and should be usable for any kind of data.

## Repository structure

* `dp3` - Python package containing code of the processing core
* `api` - HTTP API (Flask) for data ingestion and extraction
* `config` - default/example configuration
* `install` - installation scripts and files
