from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import NonNegativeInt, PositiveInt

from api.internal.config import DB, MODEL_SPEC
from api.internal.models import EntityEidData, EntityEidList
from api.internal.response_models import RequestValidationError
from dp3.common.attrspec import AttrType


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
async def get_data_of_eid(
    entity: str, eid: str, date_from: Optional[datetime] = None, date_to: Optional[datetime] = None
) -> EntityEidData:
    """Get data of `entity`'s `eid`.

    Contains all snapshots and master record.
    Snapshots are ordered by ascending creation time.
    """
    # Get master record
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
