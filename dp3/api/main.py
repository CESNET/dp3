import json

from fastapi import FastAPI
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError

from dp3.api.internal.config import DATAPOINTS_INGESTION_URL_PATH, DP_LOGGER, ROOT_PATH
from dp3.api.routers import control, entity, root

# Create new FastAPI app
app = FastAPI(root_path=ROOT_PATH)


# Redefine Pydantic validation error handler to log all bad datapoints
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    if request.url.path == DATAPOINTS_INGESTION_URL_PATH:
        # Body should by JSON
        body = exc.body if type(exc.body) is str else json.dumps(exc.body)

        DP_LOGGER.log_bad(body, str(exc), src=request.client.host)

    # Call default handler
    return await request_validation_exception_handler(request, exc)


# Register routers
app.include_router(entity.router, prefix="/entity", tags=["Entity"])
app.include_router(control.router, prefix="/control", tags=["Control"])
app.include_router(root.router)
