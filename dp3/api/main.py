import logging

from fastapi import FastAPI
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from dp3.api.internal.config import CONFIG, DATAPOINTS_INGESTION_URL_PATH, DP_LOGGER, ROOT_PATH
from dp3.api.routers import control, entity, root, telemetry

uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_access_logger = logging.getLogger("uvicorn.access")
gunicorn_error_logger = logging.getLogger("gunicorn.error")

if len(gunicorn_error_logger.handlers) > 0:  # We are running in gunicorn
    logging.root.handlers = gunicorn_error_logger.handlers
    uvicorn_access_logger.handlers = gunicorn_error_logger.handlers
elif len(uvicorn_logger.handlers) > 0:  # We are running only in uvicorn
    logging.root.handlers = uvicorn_logger.handlers

# Create new FastAPI app
app = FastAPI(root_path=ROOT_PATH)


# Redefine Pydantic validation error handler to log all bad datapoints
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    if request.url.path.replace(ROOT_PATH, "") == DATAPOINTS_INGESTION_URL_PATH:
        # Log individual errors or whole exception
        for err in exc.errors():
            # Override empty input on json decode error with request data
            if err["type"] == "json_invalid":
                err["input"] = str(exc.body)
            DP_LOGGER.log_bad(str(err), src=request.client.host)
        if not exc.errors():
            DP_LOGGER.log_bad(str(exc), src=request.client.host)

    # Call default handler
    return await request_validation_exception_handler(request, exc)


# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CONFIG.get("api.cors.allow_origins", ["*"]),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(entity.router, prefix="/entity", tags=["Entity"])
app.include_router(control.router, prefix="/control", tags=["Control"])
app.include_router(telemetry.router, prefix="/telemetry", tags=["Telemetry"])
app.include_router(root.router)
