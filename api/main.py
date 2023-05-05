from fastapi import FastAPI

from api.routers import entity, root

# Create new FastAPI app
app = FastAPI()

# Register routers
app.include_router(entity.router, prefix="/entity", tags=["Entity"])
app.include_router(root.router)
