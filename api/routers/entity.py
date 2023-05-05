from fastapi import APIRouter, Depends
from pydantic import NonNegativeInt, PositiveInt

from api.internal.config import DB, MODEL_SPEC
from api.internal.response_models import RequestValidationError


async def check_entity(entity: str):
    """Middleware to check entity existence"""
    if entity not in MODEL_SPEC.entities:
        raise RequestValidationError(["path", "entity"], f"Entity '{entity}' doesn't exist")
    return entity


router = APIRouter(dependencies=[Depends(check_entity)])


# TODO: type hint return values
@router.get("/{entity}")
async def list_entity_eids(
    entity: str, skip: NonNegativeInt = 0, limit: PositiveInt = 20
) -> list[dict]:
    """List `id`s present in database under `entity`

    Contains only latest snapshot.

    Uses pagination.
    """
    cursor = DB.get_latest_snapshots(entity).skip(skip).limit(limit)

    # Remove _id field
    result = list(cursor)
    for r in result:
        del r["_id"]
    return result
