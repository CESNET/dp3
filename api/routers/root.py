from collections import defaultdict

from fastapi import APIRouter

from api.internal.config import DB, MODEL_SPEC, TASK_WRITER
from api.internal.helpers import api_to_dp3_datapoint
from api.internal.models import DataPoint, EntityState
from api.internal.response_models import HealthCheckResponse, SuccessResponse
from dp3.common.task import DataPointTask

router = APIRouter()


@router.get("/", tags=["Health"])
async def health_check() -> HealthCheckResponse:
    """Health check

    Returns simple 'It works!' response.
    """
    return HealthCheckResponse()


@router.post("/datapoints", tags=["Data ingestion"])
async def insert_datapoints(dps: list[DataPoint]) -> SuccessResponse:
    """Insert datapoints

    Validates and pushes datapoints into task queue, so they are processed by one of DP3 workers.
    """
    # Convert to DP3 datapoints
    # This should not fail as all datapoints are already validated
    dp3_dps = [api_to_dp3_datapoint(dp.dict()) for dp in dps]
    print(dp3_dps)

    # Group datapoints by etype-eid
    tasks_dps = defaultdict(list)
    for dp in dp3_dps:
        key = (dp.etype, dp.eid)
        tasks_dps[key].append(dp)

    # Create tasks
    tasks = []
    for k in tasks_dps:
        etype, eid = k

        # This shouldn't fail either
        tasks.append(
            DataPointTask(model_spec=MODEL_SPEC, etype=etype, eid=eid, data_points=tasks_dps[k])
        )

    # Push tasks to task queue
    for task in tasks:
        TASK_WRITER.put_task(task, False)

    return SuccessResponse()


@router.get("/entities", tags=["Entity"])
async def list_entities() -> dict[str, EntityState]:
    """List entities

    Returns dictionary containing all entities configured -- their simplified configuration
    and current state information.
    """
    entities = {}

    for e_id in MODEL_SPEC.entities:
        entity_spec = MODEL_SPEC.entity(e_id)
        entities[e_id] = {
            "id": e_id,
            "name": entity_spec.name,
            "attr_count": len(MODEL_SPEC.attribs(e_id)),
            "eid_estimate_count": DB.estimate_count_eids(e_id),
        }

    return entities
