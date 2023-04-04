# DB entities

Files in `db_entities` folder describe *entities* and their *attributes*. You can think of entity as class from object-oriented programming.

Below is YAML file (e.g. `db_entities/bus.yml`) corresponding to bus tracking system example from [Data model](../../data_model/#exemplary-system) chapter.

```yaml
entity:
  id: bus
  name: Bus
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
```


## Entity

Entity is described simply by:

| Parameter  | Data-type           | Default value | Description |
|------------|---------------------|---------------|-------------|
| **`id`**   | string (identifier) | *(mandatory)* | Short string identifying the entity type, it's machine name (must match regex `[a-zA-Z_][a-zA-Z0-9_-]*`). Lower-case only is recommended. |
| **`name`** | string              | *(mandatory)* | Attribute name for humans. May contain any symbols. |


## Attributes

Each attribute is specified by the following set of parameters:


### Base

These apply to all types of attributes (plain, observations and timeseries).

| Parameter     | Data-type           | Default value | Description |
|---------------|---------------------|---------------|-------------|
| **`id`**      | string (identifier) | *(mandatory)* | Short string identifying the attribute, it's machine name (must match this regex `[a-zA-Z_][a-zA-Z0-9_-]*`). Lower-case only is recommended. |
| **`type`**    | string              | *(mandatory)* | Type of attribute. Can be either `plain`, `observations` or `timeseries`. |
| **`name`**    | string              | *(mandatory)* | Attribute name for humans. May contain any symbols. |
| `description` | string              | `""`          | Longer description of the attribute, if needed. |
| `color`       | `#xxxxxx`           | `null`        | Color to use in GUI (useful mostly for tag values), not used currently. |


### Plain-specific parameters

| Parameter             | Data-type         | Default value | Description |
|-----------------------|-------------------|---------------|-------------|
| **`data_type`**       | string            | *(mandatory)* | Data type of attribute value, see [Supported data types](#supported-data-types). |
| `categories`          | array of strings  | `null`        | List of categories if `data_type=category` and the set of possible values is known in advance and should be enforced. If not specified, any string can be stored as attr value, but only a small number of unique values are expected (which is important for display/search in GUI, for example). |
| `editable`            | bool              | `false`       | Whether value of this attribute is editable via web interface. |


### Observations-specific parameters

| Parameter             | Data-type         | Default value | Description |
|-----------------------|-------------------|---------------|-------------|
| **`data_type`**       | string            | *(mandatory)* | Data type of attribute value, see [Supported data types](#supported-data-types). |
| `categories`          | array of strings  | `null`        | List of categories if `data_type=category` and the set of possible values is known in advance and should be enforced. If not specified, any string can be stored as attr value, but only a small number of unique values are expected (which is important for display/search in GUI, for example). |
| `editable`            | bool              | `false`       | Whether value of this attribute is editable via web interface. |
| `confidence`          | bool              | `false`       | Whether a confidence value should be stored along with data value or not. |
| `multi_value`         | bool              | `false`       | Whether multiple values can be set at the same time. |
| **`history_params`**  | object, see below | *(mandatory)* | History and time aggregation parameters. A subobject with fields described in the table below. |
| `history_force_graph` | bool              | `false`       | By default, if data type of attribute is array, we show it's history on web interface as table. This option can force tag-like graph with comma-joined values of that array as tags. |

#### History params

Description of `history_params` subobject (see table above).

| Parameter       | Data-type                                  | Default value | Description |
|-----------------|--------------------------------------------|---------------|-------------|
| `max_age`       | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `null`        | How many seconds/minutes/hours/days of history to keep (older data-points/intervals are removed). |
| `max_items`     | int (> 0)                                  | `null`        | How many data-points/intervals to store (oldest ones are removed when limit is exceeded). Currently not implemented. |
| `expire_time`   | `<int><s/m/h/d>` or `inf` (infinity)       | infinity      | How long after the end time (`t2`) is the last value considered valid (i.e. is used as "current value"). Zero (`0`) means to strictly follow `t1`, `t2`. Zero can be specified without a unit (`s/m/h/d`). Currently not implemented. |
| `pre_validity`  | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `0s`          | Max time *before* `t1` for which the data-point's value is still considered to be the "current value" if there's no other data-point closer in time. |
| `post_validity` | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `0s`          | Max time *after* `t2` for which the data-point's value is still considered to be the "current value" if there's no other data-point closer in time. |

*Note: At least one of `max_age` and `max_items` SHOULD be defined, otherwise the amount of stored data can grow unbounded.*


### Timeseries-specific parameters

| Parameter             | Data-type                    | Default value | Description |
|-----------------------|------------------------------|---------------|-------------|
| **`timeseries_type`** | string                       | *(mandatory)* | One of: `regular`, `irregular` or `irregular_intervals`. See chapter *Data model* for explanation. |
| **`series`**          | object of objects, see below | *(mandatory)* | Configuration of series of data represented by this timeseries attribute. |
| `timeseries_params`   | object, see below            |               | Other timeseries parameters. A subobject with fields described by the table below. |

#### Series

Description of `series` subobject (see table above).

Key for `series` object is `id` - short string identifying the series (e.g. `bytes`, `temperature`, `parcels`).

| Parameter  | Data-type   | Default value | Description |
|------------|-------------|---------------|-------------|
| **`type`** | string      | *(mandatory)* | Data type of series. Only `int` and `float` are allowed (also `time`, but that's used internally, see below). |

Time `series` (axis) is added implicitly by DP³ and this behaviour is specific to selected `timeseries_type`:

- regular:  
  `"time": { "data_type": "time" }`
- irregular:  
  `"time": { "data_type": "time" }`
- irregular_timestamps:  
  `"time_first": { "data_type": "time" }, "time_last": { "data_type": "time" }`

#### Timeseries params

Description of `timeseries_params` subobject (see table above).

| Parameter       | Data-type                                  | Default value                                          | Description |
|-----------------|--------------------------------------------|--------------------------------------------------------|-------------|
| `max_age`       | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | `null`                                                 | How many seconds/minutes/hours/days of history to keep (older data-points/intervals are removed). |
| **`time_step`** | `<int><s/m/h/d>` (e.g. `30s`, `12h`, `7d`) | *(mandatory)* for regular timeseries, `null` otherwise | "Sampling rate in time" of this attribute. For example, with `time_step = 10m` we expect data-point at 12:00, 12:10, 12:20, 12:30,... Only relevant for regular timeseries. |

*Note: `max_age` SHOULD be defined, otherwise the amount of stored data can grow unbounded.*


### Supported data types

List of supported values for parameter `data_type`:

- `tag`: set/not_set (When the attribute is set, its value is always assumed to be `true`, the "v" field doesn't have to be stored.)
- `binary`: `true`/`false`/not_set (Attribute value is `true` or `false`, or the attribute is not set at all.)
- `category`: Categorical values. When only a fixed set of values should be allowed, these should be specified in the `categories` parameter. Otherwise the set of values is open, any string can be stored (but only a small number of unique values are expected).
- `string`
- `int`: 32-bit signed integer (range from -2147483648 to +2147483647)
- `int64`: 64-bit signed integer (use when the range of normal `int` is not sufficent)
- `float`
- `time`: Timestamp in `YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]` format or timestamp since 1.1.1970 in seconds or milliseconds.
- `ip4`: IPv4 address (passed as dotted-decimal string)
- `ip6`: IPv6 address (passed as string in short or full format)
- `mac`: MAC address (passed as string)
- `link<entity_type>`: Link to a record of the specified type, e.g. `link<ip>`
- `array<data_type>`: An array of values of specified data type (which must be one of the types above), e.g. `array<int>`
- `set<data_type>`: Same as array, but values can't repeat and order is irrelevant.
- `dict<keys>`: Dictionary (object) containing multiple values as subkeys. keys should contain a comma-separated list of key names and types separated by colon, e.g. `dict<port:int,protocol:string,tag?:string>`. By default, all fields are mandatory (i.e. a data-point missing some subkey will be refused), to mark a field as optional, put `?` after its name. Only the following data types can be used here: `binary,category,string,int,float,time,ip4,ip6,mac`. Multi-level dicts are not supported.
- `json`: Any JSON object can be stored, all processing is handled by user's code. This is here for special cases which can't be mapped to any data type above.