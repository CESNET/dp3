from collections import defaultdict

from fastapi import FastAPI

from api.internal.config import MODEL_SPEC, TASK_WRITER
from api.internal.helpers import api_to_dp3_datapoint
from api.internal.models import DataPoint, HealthCheckResponse, SuccessResponse
from api.routers import entity
from dp3.common.task import DataPointTask

# Create new FastAPI app
app = FastAPI()

# Register routers
app.include_router(entity.router, prefix="/entity", tags=["entity"])


@app.get("/")
async def health_check() -> HealthCheckResponse:
    """Health check

    Returns simple 'It works!' response.
    """
    return HealthCheckResponse()


@app.post("/datapoints")
async def insert_datapoints(dps: list[DataPoint]) -> SuccessResponse:
    """Insert datapoints

    Validates and pushes datapoints into task queue, so they are processed by one of DP3 workers.
    """
    # Convert to DP3 datapoints
    # This should not fail as all datapoints are already validated
    dp3_dps = [api_to_dp3_datapoint(dp.dict()) for dp in dps]

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
