from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import Json, NonNegativeInt, ValidationError

from dp3.api.internal.config import DB, MODEL_SPEC, TASK_WRITER
from dp3.api.internal.entity_response_models import (
    EntityEidAttrValue,
    EntityEidAttrValueOrHistory,
    EntityEidData,
    EntityEidList,
    EntityEidMasterRecord,
    EntityEidSnapshots,
    JsonVal,
)
from dp3.api.internal.helpers import api_to_dp3_datapoint
from dp3.api.internal.models import (
    DataPoint,
)
from dp3.api.internal.response_models import ErrorResponse, RequestValidationError, SuccessResponse
from dp3.common.attrspec import AttrType
from dp3.common.task import DataPointTask, task_context
from dp3.database.database import DatabaseError


async def check_etype(etype: str):
    """Middleware to check entity type existence"""
    if etype not in MODEL_SPEC.entities:
        raise RequestValidationError(["path", "etype"], f"Entity type '{etype}' doesn't exist")
    return etype


def get_eid_master_record_handler(
    etype: str, eid: str, date_from: Optional[datetime] = None, date_to: Optional[datetime] = None
):
    """Handler for getting master record of EID"""
    # TODO: This is probably not the most efficient way. Maybe gather only
    # plain data from master record and then call `get_timeseries_history`
    # for timeseries.
    master_record = DB.get_master_record(
        etype, eid, projection={"_id": False, "#hash": False, "#min_t2s": False}
    )

    entity_attribs = MODEL_SPEC.attribs(etype)

    # Get filtered timeseries data
    for attr in master_record:
        # Check for no longer existing attributes
        if attr in entity_attribs and entity_attribs[attr].t == AttrType.TIMESERIES:
            master_record[attr] = DB.get_timeseries_history(
                etype, attr, eid, t1=date_from, t2=date_to
            )

    return master_record


def get_eid_snapshots_handler(
    etype: str, eid: str, date_from: Optional[datetime] = None, date_to: Optional[datetime] = None
) -> list[dict[str, Any]]:
    """Handler for getting snapshots of EID"""
    snapshots = list(DB.get_snapshots(etype, eid, t1=date_from, t2=date_to))

    return snapshots


router = APIRouter(dependencies=[Depends(check_etype)])

# As variable, because otherwise generates ruff B008 error
eid_filter_query_param = Query(default="", deprecated=True)


@router.get(
    "/{etype}", responses={400: {"description": "Query can't be processed", "model": ErrorResponse}}
)
async def list_entity_type_eids(
    etype: str,
    eid_filter: str = eid_filter_query_param,
    fulltext_filters: Json = None,
    generic_filter: Json = None,
    skip: NonNegativeInt = 0,
    limit: NonNegativeInt = 20,
) -> EntityEidList:
    """List latest snapshots of all `id`s present in database under `etype`.

    Contains only latest snapshot.

    Uses pagination.
    Setting `limit` to 0 is interpreted as no limit (return all results).

    Returns only documents matching `generic_filter` and `fulltext_filters`
    (JSON object in format: attribute - fulltext filter).
    Fulltext filters are interpreted as regular expressions.
    Only string values may be filtered this way. There's no validation that queried attribute
    can be fulltext filtered.
    Only plain and observation attributes with string-based data types can be queried.
    Array and set data types are supported as well as long as they are not multi value
    at the same time.
    If you need to filter EIDs, use attribute `eid` (`eid_filter` is deprecated and you should
    migrate to `fulltext_filters["eid"]`).

    Generic filter allows filtering using generic MongoDB query (including `$and`, `$or`,
    `$lt`, etc.).
    For querying non-JSON-native types, you can use the following magic strings:

    - `"$$IPv4{<ip address>}"` - converts to IPv4Address object
    - `"$$IPv6{<ip address>}"` - converts to IPv6Address object
    - `"$$int{<value>}"` - may be necessary for filtering when `eid` data type is int
    - `"$$Date{<YYYY-mm-ddTHH:MM:ssZ>}"` - converts specified UTC date to UTC datetime object
    - `"$$DateTs{<POSIX timestamp>}"` - converts POSIX timestamp (int/float) to UTC datetime object
    - `"$$MAC{<mac address>}"` - converts to MACAddress object

    To query an IP prefix, use the following magic strings:

    - `"$$IPv4Prefix{<ip address>/<prefix length>}"` - matches prefix
    - `"$$IPv6Prefix{<ip address>/<prefix length>}"` - matches prefix

    To query a binary `_id`s of non-string snapshot buckets,
    use the following magic string:

    - `"$$Binary_ID{<EID object | Valid magic string>}"`

        - converts to filter the exact EID object snapshots, only EID valid types are supported

    There are no attribute name checks (may be added in the future).

    Generic filter examples:

    - `{"attr1": {"$gt": 10}, "attr2": {"$lt": 20}}`
    - `{"ip_attr": "$$IPv4{127.0.0.1}"}` - converts to IPv4Address object, exact match
    - `{"ip_attr": "$$IPv4Prefix{127.0.0.0/24}"}`
        - converts to `{"ip_attr": {"$gte": "$$IPv4{127.0.0.0}",
          "$lte": "$$IPv4{127.0.0.255}"}}`

    - `{"ip_attr": "$$IPv6{::1}"}` - converts to IPv6Address object, exact match
    - `{"ip_attr": "$$IPv6Prefix{::1/64}"}`
        - converts to `{"ip_attr": {"$gte": "$$IPv6{::1}",
          "$lte": "$$IPv6{::1:ffff:ffff:ffff:ffff}"}}`

    - `{"_id": "$$Binary_ID{$$IPv4{127.0.0.1}}"}`
        - converts to `{"_id": {"$gte": Binary(<IP bytes + min timestamp>),
          "$lt": Binary(<IP bytes + max timestamp>)}}`

    - `{"id": "$$Binary_ID{$$IPv4Prefix{127.0.0.0/24}}"}`
        - converts to `{"_id": {"$gte": Binary(<IP 127.0.0.0 bytes + min timestamp>),
          "$lte": Binary(<IP 127.0.0.255 bytes + max timestamp>)}}`

    - `{"_time_created": {"$gte": "$$Date{2024-11-07T00:00:00Z}"}}`
        - converts to `{"_time_created": datetime(2024, 11, 7, 0, 0, 0, tzinfo=timezone.utc)}`

    - `{"_time_created": {"$gte": "$$DateTs{1609459200}"}}`
        - converts to `{"_time_created": datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)}`

    - `{"attr": "$$MAC{00:11:22:33:44:55}"}` - converts to MACAddress object, exact match
    - `{"_id": "$$Binary_ID{$$MAC{Ab-cD-Ef-12-34-56}}"}`
        - converts to `{"_id": {"$gte": Binary(<MAC bytes + min timestamp>),
            "$lt": Binary(<MAC bytes + max timestamp>)}}`

    There are no attribute name checks (may be added in the future).

    Generic and fulltext filters are merged - fulltext overrides conflicting keys.
    """
    if not fulltext_filters:
        fulltext_filters = {}
    if not isinstance(fulltext_filters, dict):
        raise HTTPException(status_code=400, detail="Fulltext filter is invalid")

    if not generic_filter:
        generic_filter = {}
    if not isinstance(generic_filter, dict):
        raise HTTPException(status_code=400, detail="Generic filter is invalid")

    for attr in fulltext_filters:
        ftr = fulltext_filters[attr]
        if not isinstance(ftr, str):
            raise HTTPException(status_code=400, detail=f"Filter '{ftr}' is not string")

    # `eid_filter` is deprecated - to be removed in the future
    if eid_filter:
        fulltext_filters["eid"] = eid_filter

    try:
        cursor, total_count = DB.get_latest_snapshots(etype, fulltext_filters, generic_filter)
        cursor_page = cursor.skip(skip).limit(limit)
    except DatabaseError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    time_created = None

    # Remove _id field
    result = [r["last"] for r in cursor_page]
    for r in result:
        time_created = r["_time_created"]
        del r["_time_created"]

    return EntityEidList(
        time_created=time_created, count=len(result), total_count=total_count, data=result
    )


@router.get("/{etype}/{eid}")
async def get_eid_data(
    etype: str, eid: str, date_from: Optional[datetime] = None, date_to: Optional[datetime] = None
) -> EntityEidData:
    """Get data of `etype`'s `eid`.

    Contains all snapshots and master record.
    Snapshots are ordered by ascending creation time.

    Combines function of `/{etype}/{eid}/master` and `/{etype}/{eid}/snapshots`.
    """
    master_record = get_eid_master_record_handler(etype, eid, date_from, date_to)
    snapshots = get_eid_snapshots_handler(etype, eid, date_from, date_to)

    # Whether this eid contains any data
    empty = not master_record and len(snapshots) == 0

    return EntityEidData(empty=empty, master_record=master_record, snapshots=snapshots)


@router.get("/{etype}/{eid}/master")
async def get_eid_master_record(
    etype: str, eid: str, date_from: Optional[datetime] = None, date_to: Optional[datetime] = None
) -> EntityEidMasterRecord:
    """Get master record of `etype`'s `eid`."""
    return get_eid_master_record_handler(etype, eid, date_from, date_to)


@router.get("/{etype}/{eid}/snapshots")
async def get_eid_snapshots(
    etype: str, eid: str, date_from: Optional[datetime] = None, date_to: Optional[datetime] = None
) -> EntityEidSnapshots:
    """Get snapshots of `etype`'s `eid`."""
    return get_eid_snapshots_handler(etype, eid, date_from, date_to)


@router.get("/{etype}/{eid}/get/{attr}")
async def get_eid_attr_value(
    etype: str,
    eid: str,
    attr: str,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> EntityEidAttrValueOrHistory:
    """Get attribute value

    Value is either of:
    - current value: in case of plain attribute
    - current value and history: in case of observation attribute
    - history: in case of timeseries attribute
    """
    # Check if attribute exists
    if attr not in MODEL_SPEC.attribs(etype):
        raise RequestValidationError(["path", "attr"], f"Attribute '{attr}' doesn't exist")

    value_or_history = DB.get_value_or_history(etype, attr, eid, t1=date_from, t2=date_to)

    return EntityEidAttrValueOrHistory(attr_type=MODEL_SPEC.attr(etype, attr).t, **value_or_history)


@router.post("/{etype}/{eid}/set/{attr}")
async def set_eid_attr_value(
    etype: str, eid: str, attr: str, body: EntityEidAttrValue, request: Request
) -> SuccessResponse:
    """Set current value of attribute

    Internally just creates datapoint for specified attribute and value.

    This endpoint is meant for `editable` plain attributes -- for direct user edit on DP3 web UI.
    """
    # Check if attribute exists
    if attr not in MODEL_SPEC.attribs(etype):
        raise RequestValidationError(["path", "attr"], f"Attribute '{attr}' doesn't exist")

    # Construct datapoint
    try:
        dp = DataPoint(
            type=etype,
            id=eid,
            attr=attr,
            v=body.value,
            t1=datetime.now(),
            src=f"{request.client.host} via API",
        )
        dp3_dp = api_to_dp3_datapoint(dp.dict())
    except ValidationError as e:
        raise RequestValidationError(["body", "value"], e.errors()[0]["msg"]) from e

    # This shouldn't fail
    with task_context(MODEL_SPEC):
        task = DataPointTask(etype=etype, eid=eid, data_points=[dp3_dp])

    # Push tasks to task queue
    TASK_WRITER.put_task(task, False)

    # Datapoints from this endpoint are intentionally not logged using `DPLogger`.
    # If for some reason, in the future, they need to be, just copy code from data ingestion
    # endpoint.

    return SuccessResponse()


@router.get(
    "/{etype}/_/distinct/{attr}",
    responses={400: {"description": "Query can't be processed", "model": ErrorResponse}},
)
async def get_distinct_attribute_values(etype: str, attr: str) -> dict[JsonVal, int]:
    """Gets distinct attribute values and their counts based on latest snapshots

    Useful for displaying `<select>` enumeration fields.

    Works for all plain and observation data types except `dict` and `json`.
    """
    try:
        return DB.get_distinct_val_count(etype, attr)
    except DatabaseError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{etype}/{eid}/ttl")
async def extend_eid_ttls(etype: str, eid: str, body: dict[str, datetime]) -> SuccessResponse:
    """Extend TTLs of the specified entity"""
    # Construct task
    with task_context(MODEL_SPEC):
        task = DataPointTask(etype=etype, eid=eid, ttl_tokens=body)

    # Push tasks to task queue
    TASK_WRITER.put_task(task, False)

    return SuccessResponse()


@router.delete("/{etype}/{eid}")
async def delete_eid_record(etype: str, eid: str) -> SuccessResponse:
    """Delete the master record and snapshots of the specified entity.

    Notice that this does not delete any raw datapoints,
    or block the re-creation of the entity if new datapoints are received.
    """
    # Create a "delete" task and push it to task queue
    with task_context(MODEL_SPEC):
        task = DataPointTask(etype=etype, eid=eid, delete=True)
    TASK_WRITER.put_task(task, False)

    return SuccessResponse()
