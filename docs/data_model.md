# DP³ data model

Basic elements of the DP³ data model are *entities* (or objects), each entity
record (object instance) has a set of *attributes*.
Each attribute has some value (associated to a particular entity),
timestamp (history of previous values can be stored)
and optionally confidence value.

Entities may be mutually connected. See [Relationships](#relationships) below.

## Types of attributes

There are 3 types of attributes:

### Plain

Common attributes with only one value of some data type.
There's no history stored, but timestamp of last change is available.

Very useful for:

- data from external source, when you only need to have current value

- notes and other manually entered information

### Observations

Attributes with history of values at some time or interval of time.
Consequently, we can derive value at any time (most often not now) from these values.

Each value may have associated confidence.

These attributes may be single or multi value (multiple current values in one point in time).

Very useful for data where both current value and history is needed.

### Timeseries

One or more numeric values for a particular time.

In this attribute type: history > current value.
In fact, no explicit current value is provided.

Very useful for:

- any kind of history-based analysis

- logging of events/changes

May be:

- **regular**: sampling is regular  
  Example: datapoint is created every x minutes

- **irregular**: sampling is irregular  
  Example: datapoint is created when some event occurs

- **irregular intervals**: sampling is irregular and includes two timestamps (from when till when were provided data gathered)  
  Example: Some event triggers 5 minute monitoring routine. When this routine finishes, it creates datapoint containing all the data from past 5 minutes.

## Relationships

TODO
