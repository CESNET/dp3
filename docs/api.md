# API

DP³'s has HTTP API which you can use to post datapoints and to read data stored in DP³.
As the API is made using FastAPI, there is also an interactive documentation available at `/docs` endpoint.

There are several API endpoints:

- [`GET /`](#index): check if API is running (just returns `It works!` message)
- [`POST /datapoints`](#insert-datapoints): insert datapoints into DP³
- [`GET /entity/<entity_type>`](#list-entities): list current snapshots of all entities of given type
- [`GET /entity/<entity_type>/<entity_id>`](#get-eid-data): get data of entity with given entity id
- [`GET /entity/<entity_type>/<entity_id>/get/<attr_id>`](#get-attr-value): get attribute value
- [`GET /entity/<entity_type>/<entity_id>/set/<attr_id>`](#set-attr-value): set attribute value
- [`GET /entities`](#entities): list entity configuration
- [`GET /control/<action>`](#control): send a pre-defined action into execution queue.

---

## Index

Health check.

### Request

`GET /`

### Response

**`200 OK`**:

`{
  "detail": "It works!"
}`

---


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

---

## List entities

List latest snapshots of all ids present in database under entity type.

Contains only latest snapshot.

Uses pagination.

### Request

`GET /entity/<entity_type>`

**Optional query parameters:**

- skip: how many entities to skip (default: 0)
- limit: how many entities to return (default: 20)

### Response

```json
{
  "time_created": "2023-07-04T12:10:38.827Z",
  "data": [
    {}
  ]
}
```

---

## Get Eid data

Get data of entity type's eid.

Contains all snapshots and master record. Snapshots are ordered by ascending creation time.

### Request

`GET /entity/<entity_type>/<entity_id>`

**Optional query parameters:**

- date_from: date-time string
- date_to: date-time string

### Response

```json
{
  "empty": true,
  "master_record": {},
  "snapshots": [
    {}
  ]
}
```

---

## Get attr value


Get attribute value

Value is either of:

- current value: in case of plain attribute
- current value and history: in case of observation attribute
- history: in case of timeseries attribute

### Request

`GET /entity/<entity_type>/<entity_id>/get/<attr_id>`

**Optional query parameters:**

- date_from: date-time string
- date_to: date-time string

### Response

```json
{
  "attr_type": 1,
  "current_value": "string",
  "history": []
}
```

---

## Set attr value

Set current value of attribute

Internally just creates datapoint for specified attribute and value.

This endpoint is meant for `editable` plain attributes -- for direct user edit on DP3 web UI.

### Request

`POST /entity/<entity_type>/<entity_id>/set/<attr_id>`

**Required request body:**

```json
{
  "value": "string"
}
```

### Response

```json
{
  "detail": "OK"
}
```

---

## Entities

List entity types

Returns dictionary containing all entity types configured -- their simplified configuration and current state information.

### Request

`GET /entities`

### Response

```json
{
  "<entity_id>": {
    "id": "<entity_id>",
    "name": "<entity_spec.name>",
    "attribs": "<MODEL_SPEC.attribs(e_id)>",
    "eid_estimate_count": "<DB.estimate_count_eids(e_id)>"
  },
  ...
}
```

---

## Control

Execute Action - Sends the given action into execution queue.

You can see the enabled actions in `/config/control.yml`, available are:

- `make_snapshots` - Makes an out-of-order snapshot of all entities

### Request

`GET /control/<action>`

### Response

```json
{
  "detail": "OK"
}
```

