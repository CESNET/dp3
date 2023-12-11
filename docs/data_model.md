# DP³ data model

Basic elements of the DP³ data model are *entity types* (like classes), each
*entity* (or class instance) has a set of *attributes*.
Each attribute has some value (associated to a particular entity type),
timestamp (history of previous values can be stored)
and optionally confidence value.

Entities may be mutually connected. See [Relationships](#relationships) below.

## Exemplary system

In this chapter, we will illustrate details on an exemplary system. Imagine you
are developing data model for bus tracking system. You have to store these data:

- **label**: Custom label for the bus set by administrator in web interface.
- **location**: Location of the bus in a particular time. Value are GPS
  coordinates (array of latitude and longitude).
- **speed**: Speed of the bus in a particular time.
- **passengers getting in and out**: Number of passengers getting in or out of
  the bus. Distinguished by the doors used (front, middle, back). Bus control
  unit sends counters value every 10 minutes.

Also, map displaying current position of all buses is required.

(In case you are interested, configuration of database entities for this system
is available in [DB entities](../configuration/db_entities/) chapter.)

To make everything clear and more readable, all example references below are
typesetted as quotes.

## Types of attributes

There are 3 types of attributes:

### Plain

Common attributes with only one value of some data type.
There's no history stored, but timestamp of last change is available.

Very useful for:

- data from external source, when you only need to have current value

- notes and other manually entered information

> This is exactly what we need for **label** in our [bus tracking system](#exemplary-system).
> Administor labels particular bus inside web interface and we use this label
> until it's changed - particularly display label next to a marker on a map.
> No history is needed and it has 100% confidence.

Detailed information about **plain** attribute configuration can be found in [DB configuration](configuration/db_entities.md#plain-specific-parameters).

### Observations

Attributes with history of values at some time or interval of time.
Consequently, we can derive value at any time (most often not now) from these values.

Each value may have associated confidence.

These attributes may be single or multi value (multiple current values in one point in time).

Very useful for data where both current value and history is needed.

> In our [example](#exemplary-system), **location** is great use-case for observations type.
> We need to track position of the bus in time and store the history. Current
> location is very important. Let's suppose, we also need to do oversampling by
> predicting where is the bus now, eventhout we received last data-point 2 minutes
> ago. This is all possible (predictions using custom secondary modules).
>
> The same applies to **speed**. It can also be derived from location.

Detailed information about **observations** attribute configuration can be found in [DB configuration](configuration/db_entities.md#observations-specific-parameters).
The [History mangement](history_management.md) page describes how DP³ handles history.

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

> Timeseries are very useful for **passengers getting in and out** (from our [example](#exemplary-system)).
> As we need to count two directions (in/out) for three doors (front/middle/back),
> we create 6 series (e.g. `front_in`, `front_out`, ..., `back_out`).
> Counter data-points are received in 10 minute interval, so regular timeseries
> are best fit for this use-case.
> Every 10 minutes we receive values for all 6 series and store them.
> Current value is not important as these data are only useful for passenger
> flow analysis throught whole month/year/...

Detailed information about **timeseries** attribute configuration can be found in [DB configuration](configuration/db_entities.md#timeseries-specific-parameters).

## Relationships

Relationships between entities can be represented with or without history.
They are realized using the **link** attribute type.
Depedning on whether the history is important, they can be configured using as the mentioned
**plain** data or **observations**.

Relationships **can contain additional data**, if that fits the modelling needs of your use case. 

Very useful for:

- any kind of relationship between entities
- linkning dynamic entities to entities with static data

Relationships can also be **mirrored**, meaning that the having the relationship from entity `A` to entity `B`
will automatically create the relationship from entity `B` to entity `A` in snapshots.
This is useful if you need to track a relationship in both directions, but managing both directions is not reasonable.

!!! info "Did you know?"
    
    Relationships can be placed into arrays and sets, just like any other attribute type.
    This allows you to model many-to-many relationships without the need for using a multi-value attribute type.

> As our example so far contains only one entity type, we currently have no need for relationships.
> However, if we wanted to track the different **bus drivers** driving individual buses, 
> relationships would come in quite handy. 
> The bus driver is a separate entity type, and can drive multiple buses during the day.
> The current bus driver will be represented as an observation link between the bus and the driver,
> as can be seen in the [resulting configuration](../configuration/db_entities/).

[//]: # (TODO more info?)

## Continue to ...

Now that you have an understanding of the data model and the types of attributes,
you might want to check out the details of [DB configuration](../configuration/db_entities/),
where you will find the **parameters for each attribute type**
and the **data types** supported by the platform.

