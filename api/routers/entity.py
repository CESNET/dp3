from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Request
from pydantic import NonNegativeInt, PositiveInt, ValidationError

from api.internal.config import DB, MODEL_SPEC, TASK_WRITER
from api.internal.entity_response_models import (
    EntityEidAttrValue,
    EntityEidAttrValueOrHistory,
    EntityEidData,
    EntityEidList,
)
from api.internal.helpers import api_to_dp3_datapoint
from api.internal.models import (
    DataPoint,
)
from api.internal.response_models import RequestValidationError, SuccessResponse
from dp3.common.attrspec import AttrType
from dp3.common.task import DataPointTask


async def check_entity(entity: str):
    """Middleware to check entity existence"""
    if entity not in MODEL_SPEC.entities:
        raise RequestValidationError(["path", "entity"], f"Entity '{entity}' doesn't exist")
    return entity


router = APIRouter(dependencies=[Depends(check_entity)])


@router.get("/{entity}")
async def list_entity_eids(
    entity: str, skip: NonNegativeInt = 0, limit: PositiveInt = 20
) -> EntityEidList:
    """List `id`s present in database under `entity`

    Contains only latest snapshot.

    Uses pagination.
    """
    cursor = DB.get_latest_snapshots(entity).skip(skip).limit(limit)

    time_created = None

    # Remove _id field
    result = list(cursor)
    for r in result:
        time_created = r["_time_created"]
        del r["_time_created"]
        del r["_id"]

    return EntityEidList(time_created=time_created, data=result)


@router.get("/{entity}/{eid}")
async def get_eid_data(
    entity: str, eid: str, date_from: Optional[datetime] = None, date_to: Optional[datetime] = None
) -> EntityEidData:
    """Get data of `entity`'s `eid`.

    Contains all snapshots and master record.
    Snapshots are ordered by ascending creation time.
    """
    # Get master record
    # TODO: This is probably not the most efficient way. Maybe gather only
    # plain data from master record and then call `get_timeseries_history`
    # for timeseries.
    master_record = DB.get_master_record(entity, eid)
    if "_id" in master_record:
        del master_record["_id"]

    # Get filtered timeseries data
    for attr in master_record:
        if MODEL_SPEC.attr(entity, attr).t == AttrType.TIMESERIES:
            master_record[attr] = DB.get_timeseries_history(
                entity, attr, eid, t1=date_from, t2=date_to
            )

    # Get snapshots
    snapshots = list(DB.get_snapshots(entity, eid, t1=date_from, t2=date_to))
    for s in snapshots:
        del s["_id"]

    # Whether this eid contains any data
    empty = not master_record and len(snapshots) == 0

    return EntityEidData(empty=empty, master_record=master_record, snapshots=snapshots)


@router.get("/{entity}/{eid}/get/{attr}")
async def get_eid_attr_value(
    entity: str,
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
    if attr not in MODEL_SPEC.attribs(entity):
        raise RequestValidationError(["path", "attr"], f"Attribute '{attr}' doesn't exist")

    value_or_history = DB.get_value_or_history(entity, attr, eid, t1=date_from, t2=date_to)

    return EntityEidAttrValueOrHistory(
        attr_type=MODEL_SPEC.attr(entity, attr).t, **value_or_history
    )


@router.post("/{entity}/{eid}/set/{attr}")
async def set_eid_attr_value(
    entity: str, eid: str, attr: str, body: EntityEidAttrValue, request: Request
) -> SuccessResponse:
    """Set current value of attribute

    Internally just creates datapoint for specified attribute and value.

    This endpoint is meant for `editable` plain attributes -- for direct user edit on DP3 web UI.
    """
    # Check if attribute exists
    if attr not in MODEL_SPEC.attribs(entity):
        raise RequestValidationError(["path", "attr"], f"Attribute '{attr}' doesn't exist")

    # Construct datapoint
    try:
        dp = DataPoint(
            type=entity,
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
    task = DataPointTask(model_spec=MODEL_SPEC, etype=entity, eid=eid, data_points=[dp3_dp])

    # Push tasks to task queue
    TASK_WRITER.put_task(task, False)

    # Datapoints from this endpoint are intentionally not logged using `DPLogger`.
    # If for some reason, in the future, they need to be, just copy code from data ingestion
    # endpoint.

    return SuccessResponse()
