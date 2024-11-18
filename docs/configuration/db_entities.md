# DB entities

Files in `db_entities` folder describe *entities* and their *attributes*. You can think of entity as class from object-oriented programming. 
This serves as sort of schema for the database DP³ uses.
How DP³ deals with changes to the `db_entities` is described in the [schema tracking](#schema-tracking) section.

Below is YAML file (e.g. `db_entities/bus.yml`) corresponding to bus tracking system example from [Data model](../../data_model/#exemplary-system) chapter.

```yaml
entity:
  id: bus
  name: Bus
  snapshot: true
attribs:
  # Attribute `label`
  label:
    name: Label
    description: Custom label for the bus.
    type: plain
    data_type: string
    editable: true

  # Attribute `location`
  location:
    name: Location
    description: Location of the bus in a particular time. Value are GPS \
      coordinates (array of latitude and longitude).
    type: observations
    data_type: array<float>
    history_params:
      pre_validity: 1m
      post_validity: 1m
      max_age: 30d

  # Attribute `speed`
  speed:
    name: Speed
    description: Speed of the bus in a particular time. In km/h.
    type: observations
    data_type: float
    history_params:
      pre_validity: 1m
      post_validity: 1m
      max_age: 30d

  # Attribute `passengers_in_out`
  passengers_in_out:
    name: Passengers in/out
    description: Number of passengers getting in or out of the bus. Distinguished by the doors used (front, middle, back). Regularly sampled every 10 minutes.
    type: timeseries
    timeseries_type: regular
    timeseries_params:
      max_age: 14d
    time_step: 10m
    series:
      front_in:
        data_type: int
      front_out:
        data_type: int
      middle_in:
        data_type: int
      middle_out:
        data_type: int
      back_in:
        data_type: int
      back_out:
        data_type: int

  # Attribute `driver` to link the driver of the bus at a given time.
  driver:
    name: Driver
    description: Driver of the bus at a given time.
    type: observations
    data_type: link<driver>
    history_params:
      pre_validity: 1m
      post_validity: 1m
      max_age: 30d
```


## Entity

Entity is described simply by:

| Parameter      | Data-type           | Default value       | Description                                                                                                                               |
|----------------|---------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| **`id`**       | string (identifier) | *(mandatory)*       | Short string identifying the entity type, it's machine name (must match regex `[a-zA-Z_][a-zA-Z0-9_-]*`). Lower-case only is recommended. |
| `id_data_type` | string              | "string"            | Data type of the entity id (`eid`) value, see [Supported eid data types](#supported-entity-id-data-types).                                |
| **`name`**     | string              | *(mandatory)*       | Attribute name for humans. May contain any symbols.                                                                                       |
| **`snapshot`** | bool                | *(mandatory)*       | Whether to create snapshots of the entity. See [Architecture](../architecture.md#data-flow) for more details.                             |
| `lifetime`     | `Lifetime Spec`     | `Immortal Lifetime` | Defines the lifetime of the entitiy, entities are never deleted by default. See the [Entity Lifetimes](lifetimes.md) for details.         |

### Supported entity id data types

Only a subset of [primitive data types](#primitive-types) is supported for entity ids. The supported data types are:

- `string` (default)
- `int`: 32-bit signed integer (range from -2147483648 to +2147483647)
- `ipv4`: IPv4 address, represented as [IPv4Address][ipaddress.IPv4Address] (passed as dotted-decimal string)
- `ipv6`: IPv6 address, represented as [IPv6Address][ipaddress.IPv6Address] (passed as string in short or full format)
- `mac`: MAC address, represented as [MACAddress][dp3.common.mac_address.MACAddress]  (passed as string)

Whenever writing a piece of code independent of a specific configuration,
the [`AnyEidT`][dp3.common.datatype.AnyEidT] type alias should be used.

## Attributes

Each attribute is specified by the following set of parameters:


### Base

These apply to all types of attributes (plain, observations and timeseries).

| Parameter     | Data-type           | Default value | Description                                                                                                                                                                                                                                       |
|---------------|---------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`id`**      | string (identifier) | *(mandatory)* | Short string identifying the attribute, it's machine name (must match this regex `[a-zA-Z_][a-zA-Z0-9_-]*`). Lower-case only is recommended.                                                                                                      |
| **`type`**    | string              | *(mandatory)* | Type of attribute. Can be either `plain`, `observations` or `timeseries`.                                                                                                                                                                         |
| **`name`**    | string              | *(mandatory)* | Attribute name for humans. May contain any symbols.                                                                                                                                                                                               |
| `ttl`         | timedelta           | `0`           | Optional extension of TTL of the entity, will be ignored if the [lifetime type](lifetimes.md#time-to-live-tokens) does not match. The time extension is calculated from `t2` if possible, otherwise from the current time (for plain attributes). |
| `description` | string              | `""`          | Longer description of the attribute, if needed.                                                                                                                                                                                                   |


### Plain-specific parameters

| Parameter       | Data-type        | Default value | Description                                                                                                                                                                                                                                                                                        |
|-----------------|------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`data_type`** | string           | *(mandatory)* | Data type of attribute value, see [Supported data types](#supported-data-types).                                                                                                                                                                                                                   |
| `editable`      | bool             | `false`       | Whether value of this attribute is editable via web interface.                                                                                                                                                                                                                                     |


### Observations-specific parameters

| Parameter             | Data-type         | Default value | Description                                                                                                                                                                                   |
|-----------------------|-------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`data_type`**       | string            | *(mandatory)* | Data type of attribute value, see [Supported data types](#supported-data-types).                                                                                                              |
| `editable`            | bool              | `false`       | Whether value of this attribute is editable via web interface.                                                                                                                                |
| `confidence`          | bool              | `false`       | Whether a confidence value should be stored along with data value or not. [More details](../history_management.md#confidence).                                                                |
| `multi_value`         | bool              | `false`       | Whether multiple values can be set at the same time. [More details](../history_management.md#multi-value-data-points), [Arrays vs Multi-value attributes](#arrays-vs-multi-value-attributes). |
| **`history_params`**  | object, see below | *(mandatory)* | History and time aggregation parameters. A subobject with fields described in the table below.                                                                                                |
| `history_force_graph` | bool              | `false`       | By default, if data type of attribute is array, we show it's history on web interface as table. This option can force tag-like graph with comma-joined values of that array as tags.          |

#### History params

Description of `history_params` subobject (see table above).

| Parameter       | Data-type                                  | Default value | Description                                                                                                                                                                                                                           |
|-----------------|--------------------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `max_age`       | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `null`        | How many seconds/minutes/hours/days of history to keep (older data-points/intervals are removed).                                                                                                                                     |
| `max_items`     | int (> 0)                                  | `null`        | How many data-points/intervals to store (oldest ones are removed when limit is exceeded). Currently not implemented.                                                                                                                  |
| `expire_time`   | `<int><s/m/h/d>` or `inf` (infinity)       | infinity      | How long after the end time (`t2`) is the last value considered valid (i.e. is used as "current value"). Zero (`0`) means to strictly follow `t1`, `t2`. Zero can be specified without a unit (`s/m/h/d`). Currently not implemented. |
| `pre_validity`  | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `0s`          | Max time *before* `t1` for which the data-point's value is still considered to be the ["current value"](../history_management.md) if there's no other data-point closer in time.                                                      |
| `post_validity` | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `0s`          | Max time *after* `t2` for which the data-point's value is still considered to be the ["current value"](../history_management.md) if there's no other data-point closer in time.                                                       |
| `aggregate`     | `bool`                                     | `true`        | Whether to aggregate data-points in DB master records. Currently only identical value aggregation is supported. [More details](../history_management.md#data-point-merging)                                                           |

*Note: At least one of `max_age` and `max_items` SHOULD be defined, otherwise the amount of stored data can grow unbounded.*


### Timeseries-specific parameters

| Parameter             | Data-type                    | Default value | Description                                                                                        |
|-----------------------|------------------------------|---------------|----------------------------------------------------------------------------------------------------|
| **`timeseries_type`** | string                       | *(mandatory)* | One of: `regular`, `irregular` or `irregular_intervals`. See chapter *Data model* for explanation. |
| **`series`**          | object of objects, see below | *(mandatory)* | Configuration of series of data represented by this timeseries attribute.                          |
| `timeseries_params`   | object, see below            |               | Other timeseries parameters. A subobject with fields described by the table below.                 |

#### Series

Description of `series` subobject (see table above).

Key for `series` object is `id` - short string identifying the series (e.g. `bytes`, `temperature`, `parcels`).

| Parameter  | Data-type | Default value | Description                                                                                                   |
|------------|-----------|---------------|---------------------------------------------------------------------------------------------------------------|
| **`type`** | string    | *(mandatory)* | Data type of series. Only `int` and `float` are allowed (also `time`, but that's used internally, see below). |

Time `series` (axis) is added implicitly by DP³ and this behaviour is specific to selected `timeseries_type`:

- regular:  
  `"time": { "data_type": "time" }`
- irregular:  
  `"time": { "data_type": "time" }`
- irregular_timestamps:  
  `"time_first": { "data_type": "time" }, "time_last": { "data_type": "time" }`

#### Timeseries params

Description of `timeseries_params` subobject (see table above).

| Parameter       | Data-type                                  | Default value                                          | Description                                                                                                                                                                 |
|-----------------|--------------------------------------------|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `max_age`       | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `null`                                                 | How many seconds/minutes/hours/days of history to keep (older data-points/intervals are removed).                                                                           |
| **`time_step`** | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | *(mandatory)* for regular timeseries, `null` otherwise | "Sampling rate in time" of this attribute. For example, with `time_step = 10m` we expect data-point at 12:00, 12:10, 12:20, 12:30,... Only relevant for regular timeseries. |

*Note: `max_age` SHOULD be defined, otherwise the amount of stored data can grow unbounded.*


### Supported data types

List of supported values for parameter `data_type`:

#### Primitive types

- `tag`: set/not_set (When the attribute is set, its value is always assumed to be `true`, the "v" field doesn't have to be stored.)
- `binary`: `true`/`false`/not_set (Attribute value is `true` or `false`, or the attribute is not set at all.)
- `string`
- `int`: 32-bit signed integer (range from -2147483648 to +2147483647)
- `int64`: 64-bit signed integer (use when the range of normal `int` is not sufficent)
- `float`
- `time`: Timestamp in `YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]` format or timestamp since 1.1.1970 in seconds or milliseconds.
- `ipv4`: IPv4 address, represented as [IPv4Address][ipaddress.IPv4Address] (passed as dotted-decimal string)
- `ipv6`: IPv6 address, represented as [IPv6Address][ipaddress.IPv6Address] (passed as string in short or full format)
- `mac`: MAC address, represented as [MACAddress][dp3.common.mac_address.MACAddress]  (passed as string)
- `json`: Any JSON object can be stored, all processing is handled by user's code. This is here for special cases which can't be mapped to any other data type.

#### Composite types

- `category<data_type; category1, category2, ...>`: Categorical values. Use only when a fixed set of values should be allowed, which should be specified in the second part of the type definition. The first part of the type definition describes the data_type of the category.
- `array<data_type>`: An array of values of specified data type (which must be one of the primitive types above or a [link to another entity](#relationships)), e.g. `array<int>`. Deciding whether to use array or multi-value attribute is not always trivial, see [Arrays vs Multi-value attributes](#arrays-vs-multi-value-attributes).
- `set<data_type>`: Same as array, but values can't repeat and order is irrelevant.
- `dict<keys>`: Dictionary (object) containing multiple values as subkeys. keys should contain a comma-separated list of key names and types separated by colon, e.g. `dict<port:int, protocol:string, tag?:string>`. Whitespace is allowed after colons. By default, all fields are mandatory (i.e. a data-point missing some subkey will be refused), to mark a field as optional, put `?` after its name. Only the primitive data types can be used here, multi-level dicts are not supported.

#### Relationships

- `link<entity_type>`: Link to a record of the specified type, e.g. `link<ip>`
- `link<entity_type,data_type>`: Link to a record of the specified type, carrying additional data, e.g. `link<ip,int>`
- `link<...;mirror=attr_name>`: A **mirrored** link - at the end of link specification, 
  you may enter a `mirror=attr_name` declaration, where `attr_name` is the name of an attribute in the linked entity.
  This attribute will be automatically defined in the target entity, do not define it in configuration. When a relationship
  is **mirrored**, the relationship from entity `A` to entity `B` will automatically create a 
  relationship from entity `B` to entity `A` in snapshots.
  This is useful if you need to track a relationship in both directions, but managing both directions is not reasonable.

## Schema Tracking

In order to maintain a consistent database state, DP³ tracks changes to the `db_entities` folder.
The current schema is stored in the database, and is updated automatically on worker start-up
when the `db_entities` folder is changed.

For additive changes (adding new entities or attributes), the changes are applied automatically,
as only the schema itself needs to be modified.
For changes that would require modification to the entity collections
(e.g. changing the data-type of an attribute or deleting it), 
the changes are not applied automatically to protect the database contents against accidental deletion.
The workers will refuse to start, prompting you to run `dp3 schema-update` in their logs.
You then have to run `dp3 schema-update` manually to confirm the application of the changes.
Find out more about the `dp3 schema-update` using the `--help` option.

## FAQ

### Arrays vs Multi-value attributes

Let's say you have data in an array, and are unsure how to model it into DP³ attributes.

Choose [`data_type: array<...>`](#composite-types) when:

* You're working with **numerical data**,
* The list is **ordered**, and the order is important (or you're modelling a **tuple**),
* The individual values in the list generally do not repeat between different datapoints, and 
  does not make sense to aggregate them.

In that case, you should set `multi_value` to `false`, and DP³ will handle the data as a single value.
You are responsible for not sending overlapping datapoints, where a datapoint contains **the whole array**.
The aggregation of this attribute will be limited, but that is usually desirable.

Choose [`multi_value: true`](#observations-specific-parameters) when:

* You're working with elements of **categorical** data or data that has a **composite type** (i.e. a struct or dictionary),
* The list is **unordered** or the order is not important,
* The individual values in the list generally repeat between different datapoints,
  represent some state of the entity, and it makes sense to aggregate them.

Then you should set the `data_type` to the element type.
Your datapoints should contain **a single value**, but DP³ will handle the data as a list of values,
and you can send overlapping datapoints. 
Value aggregation will be done on a per-element basis.