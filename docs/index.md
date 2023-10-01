# Dynamic Profile Processing Platform (DP³)

<figure markdown>
  ![DP3 logo](img/dp3-logo-min.svg){ width="300" }
</figure>

DP³ is a platform helps to keep a database of information (attributes) about individual
entities (designed for IP addresses and other network identifiers, but may be anything),
when the data constantly changes in time.

DP³ doesn't do much by itself, it must be supplemented by application-specific modules providing
and processing data.

This is a basis of CESNET's "Asset Discovery Classification and Tagging" (ADiCT) project,
focused on discovery and classification of network devices,
but the platform itself is general and should be usable for any kind of data.

For an introduction about how it works, see please check out the 
[architecture](architecture.md), [data-model](data_model.md) 
and [database config](configuration/db_entities.md) pages.
The mechanics of how the platform handles history of data is described in the [history management](history_management.md) page.

Then you should be able to create a DP³ app using the provided setup utility as described in the [install](install.md) page and start tinkering!

## Repository structure

* `dp3` - Python package containing code of the processing core and the API
* `config` - default/example configuration
* `install` - deployment configuration

