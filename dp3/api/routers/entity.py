from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import Json, NonNegativeInt, PositiveInt, ValidationError

from dp3.api.internal.config import DB, MODEL_SPEC, TASK_WRITER
from dp3.api.internal.entity_response_models import (
    EntityEidAttrValue,
    EntityEidAttrValueOrHistory,
    EntityEidData,
    EntityEidList,
)
from dp3.api.internal.helpers import api_to_dp3_datapoint
from dp3.api.internal.models import (
    DataPoint,
)
from dp3.api.internal.response_models import ErrorResponse, RequestValidationError, SuccessResponse
from dp3.common.attrspec import AttrType
from dp3.common.task import DataPointTask
from dp3.database.database import DatabaseError


async def check_etype(etype: str):
    """Middleware to check entity type existence"""
    if etype not in MODEL_SPEC.entities:
        raise RequestValidationError(["path", "etype"], f"Entity type '{etype}' doesn't exist")
    return etype


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
    skip: NonNegativeInt = 0,
    limit: PositiveInt = 20,
) -> EntityEidList:
    """List latest snapshots of all `id`s present in database under `etype`.

    Contains only latest snapshot.

    Uses pagination.

    Returns only documents matching `fulltext_filters`
    (JSON object in format: attribute - fulltext filter).
    These filters are interpreted as regular expressions.
    Only string values may be filtered this way. There's no validation that queried attribute
    can be fulltext filtered.
    Only plain and observation attributes with string-based data types can be queried.
    Array and set data types are supported as well as long as they are not multi value
    at the same time.
    If you need to filter EIDs, use attribute `eid` (`eid_filter` is deprecated and you should
    migrate to `fulltext_filters["eid"]`).
    """
    if not fulltext_filters:
        fulltext_filters = {}

    if not isinstance(fulltext_filters, dict):
        raise HTTPException(status_code=400, detail="Fulltext filter is invalid")

    for attr in fulltext_filters:
        ftr = fulltext_filters[attr]
        if not isinstance(ftr, str):
            raise HTTPException(status_code=400, detail=f"Filter '{ftr}' is not string")

    # `eid_filter` is deprecated - to be removed in the future
    if eid_filter:
        fulltext_filters["eid"] = eid_filter

    try:
        cursor, total_count = DB.get_latest_snapshots(etype, fulltext_filters)
        cursor_page = cursor.skip(skip).limit(limit)
    except DatabaseError as e:
        raise HTTPException(status_code=400, detail="Query is invalid") from e

    time_created = None

    # Remove _id field
    result = list(cursor_page)
    for r in result:
        time_created = r["_time_created"]
        del r["_time_created"]
        del r["_id"]

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
    """
    # Get master record
    # TODO: This is probably not the most efficient way. Maybe gather only
    # plain data from master record and then call `get_timeseries_history`
    # for timeseries.
    master_record = DB.get_master_record(etype, eid)
    if "_id" in master_record:
        del master_record["_id"]
    if "#hash" in master_record:
        del master_record["#hash"]

    entity_attribs = MODEL_SPEC.attribs(etype)

    # Get filtered timeseries data
    for attr in master_record:
        # Check for no longer existing attributes
        if attr in entity_attribs and entity_attribs[attr].t == AttrType.TIMESERIES:
            master_record[attr] = DB.get_timeseries_history(
                etype, attr, eid, t1=date_from, t2=date_to
            )

    # Get snapshots
    snapshots = list(DB.get_snapshots(etype, eid, t1=date_from, t2=date_to))
    for s in snapshots:
        del s["_id"]

    # Whether this eid contains any data
    empty = not master_record and len(snapshots) == 0

    return EntityEidData(empty=empty, master_record=master_record, snapshots=snapshots)


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
    task = DataPointTask(model_spec=MODEL_SPEC, etype=etype, eid=eid, data_points=[dp3_dp])

    # Push tasks to task queue
    TASK_WRITER.put_task(task, False)

    # Datapoints from this endpoint are intentionally not logged using `DPLogger`.
    # If for some reason, in the future, they need to be, just copy code from data ingestion
    # endpoint.

    return SuccessResponse()


@router.post("/{etype}/{eid}/ttl")
async def extend_eid_ttls(etype: str, eid: str, body: dict[str, datetime]) -> SuccessResponse:
    """Extend TTLs of the specified entity"""
    # Construct task
    task = DataPointTask(model_spec=MODEL_SPEC, etype=etype, eid=eid, ttl_tokens=body)

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
    task = DataPointTask(model_spec=MODEL_SPEC, etype=etype, eid=eid, delete=True)
    TASK_WRITER.put_task(task, False)

    return SuccessResponse()
