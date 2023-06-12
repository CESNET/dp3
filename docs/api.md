# API

DP³'s has HTTP API which you can use to post datapoints and to read data stored in DP³.

There are several API endpoints:

- [`GET /`](#index): check if API is running (just returns `It works!` message)
- [`POST /datapoints`](#insert-datapoints): insert datapoints into DP³
- [`GET /<entity_type>/<entity_id>/<attr_id>`](#attribute-value): read current value for an attribute
- [`GET /<entity_type>/<entity_id>/<attr_id>/history`](#attribute-history): read history of an attribute
- [`GET /workers_alive`](#workers-alive): check whether any workers are running

## Index

Health check.

### Request

`GET /`

### Response

**`200 OK`**:

`It works!`

## Insert datapoints

### Request

`POST /datapoints`

All data are written to DP³ in the form of **datapoints**. A datapoint sets a value of a given attribute of given entity.

It is a JSON-encoded object with the set of keys defined in the table below. Presence of some keys depends on the primary type of the attribute (plain/observations/timseries).

Payload to this endpoint is JSON array of datapoints. For example:

```json
[
   { DATAPOINT1 },
   { DATAPOINT2 }
]
```

| Key    | Description                                                      | Data-type                | Required?              | Plain | Observations | Timeseries |
|--------|------------------------------------------------------------------|--------------------------|------------------------|-------|--------------|------------|
| `type` | Entity type                                                      | string                   | mandatory              | ✔     | ✔            | ✔          |
| `id`   | Entity identification                                            | string                   | mandatory              | ✔     | ✔            | ✔          |
| `attr` | Attribute name                                                   | string                   | mandatory              | ✔     | ✔            | ✔          |
| `v`    | The value to set, depends on attr. type and data-type, see below | --                       | mandatory              | ✔     | ✔            | ✔          |
| `t1`   | Start time of the observation interval                           | string (RFC 3339 format) | mandatory              | --    | ✔            | ✔          |
| `t2`   | End time of the observation interval                             | string (RFC 3339 format) | optional, default=`t1` | --    | ✔            | ✔          |
| `c`    | Confidence                                                       | float (0.0-1.0)          | optional, default=1.0  | --    | ✔            | ✔          |
| `src`  | Identification of the information source                         | string                   | optional, default=""   | ✔     | ✔            | ✔          |

More details depends on the particular type of the attribute.

#### Examples of datapoints

##### Plain

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "note",
  "v": "My home router",
  "src": "web_gui"
}
```

##### Observations

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "open_ports",
  "v": [22, 80, 443],
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:10:00",
  "src": "open_ports_module"
}
```

##### Timeseries

**`regular`**:

```json
{
  ...
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:20:00", // assuming time_step = 5 min
  "v": {
    "a": [1, 3, 0, 2]
  }
}
```

**`irregular`**: timestamps must always be present

```json
{
  ...
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:05:00",
  "v": {
    "time": ["2022-08-01T12:00:00", "2022-08-01T12:01:10", "2022-08-01T12:01:15", "2022-08-01T12:03:30"],
    "x": [0.5, 0.8, 1.2, 0.7],
    "y": [-1, 3, 0, 0]
  }
}
```

##### Relations

Can be represented using both **plain** attributes and **observations**. The difference will be only
in time specification. Two examples using observations:

**no data - `link<mac>`**: just the eid is sent

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "mac_addrs",
  "v": "AA:AA:AA:AA:AA",
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:10:00"
}
```

**with additional data - `link<ip, int>`**: The eid and the data are sent as a dictionary.

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "ip_dep",
  "v": {"eid": "192.168.0.2", "data": 22},
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:10:00"
}
```

**`irregular_interval`**:

```json
{
  ...
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:05:00",
  "v": {
    "time_first": ["2022-08-01T12:00:00", "2022-08-01T12:01:10", "2022-08-01T12:01:15", "2022-08-01T12:03:30"],
    "time_last": ["2022-08-01T12:01:00", "2022-08-01T12:01:15", "2022-08-01T12:03:00", "2022-08-01T12:03:40"],
    "x": [0.5, 0.8, 1.2, 0.7],
    "y": [-1, 3, 0, 0]
  }
}
```

### Response

**`200 OK`**:

```
Success
```

**`400 Bad request`**:

Returns some validation error message, for example:

```
1 validation error for DataPointObservations_some_field
v -> some_embedded_dict_field
  field required (type=value_error.missing)
```

## Attribute value

### Request

`GET /<entity_type>/<entity_id>/<attr_id>`

Currently not working. TODO

### Response

TODO

## Attribute history

### Request

`GET /<entity_type>/<entity_id>/<attr_id>/history`

**Optional query parameters:**

- `t1`: history since what time to return
- `t2`: history until what time to return

Example usage:

```
/ip/1.2.3.4/test_attr/history?t1=2020-01-23T12:00:00&t2=2020-01-23T14:00:00
/ip/1.2.3.4/test_attr/history?t1=2020-01-23T12:00:00
/ip/1.2.3.4/test_attr/history?t2=2020-01-23T14:00:00
/ip/1.2.3.4/test_attr/history
```

### Response

**`200 OK`**:

```json
[
  {
    "c": 1.0,
    "t1": "Fri, 24 Feb 2023 08:45:46 GMT",
    "t2": "Fri, 24 Feb 2023 09:45:46 GMT",
    "v": "value1"
  },
  {
    "c": 0.84,
    "t1": "Fri, 24 Feb 2023 19:49:49 GMT",
    "t2": "Fri, 24 Feb 2023 20:49:49 GMT",
    "v": "value2"
  },
  {
    "c": 0.991,
    "t1": "Sat, 25 Feb 2023 01:40:46 GMT",
    "t2": "Sat, 25 Feb 2023 02:40:46 GMT",
    "v": "value3"
  }
]
```

**`400 Bad request`**:

```
Error: no attribute specification found for 'some_attribute'
```

## Workers alive

### Request

`GET /workers_alive`

Checks the RabbitMQ statistics twice, if there is any difference,
a live worker is assumed.

### Response

**`200 OK`**:

```json
{
  "workers_alive": true,
  "deliver_get_difference": 15
}
```
