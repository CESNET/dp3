from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, root_validator, validator
from pydantic.error_wrappers import ValidationError

from dp3.common.config import ModelSpec
from dp3.common.datapoint import DataPointBase


class Task(BaseModel, ABC):
    """
    A generic task type class.

    An abstraction for the [task queue][dp3.task_processing.task_queue] classes to depend upon.
    """

    @abstractmethod
    def routing_key(self):
        """
        Returns:
            A hashable object to be used as a routing key between workers.
        """

    @abstractmethod
    def as_message(self) -> str:
        """
        Returns:
            A string representation of the object.
        """


class Push(Task):
    """Push

    Contains single task to be pushed to TaskQueue and processed.
    Attributes:
        etype: Entity type
        ekey: Entity id / key
        data_points: List of DataPoints to process
        tags: List of tags
        ttl_token: ...
    """

    # Model specification just for internal validation. Discarded after that.
    model_spec: ModelSpec

    etype: str
    ekey: str
    data_points: list[DataPointBase] = []
    tags: list[Any] = []
    ttl_token: Optional[datetime] = None

    def routing_key(self):
        return f"{self.etype}:{self.ekey}"

    def as_message(self) -> str:
        return self.json(exclude={"model_spec"})

    @validator("etype")
    def validate_etype(cls, v, values):
        if "model_spec" in values:
            assert v in values["model_spec"], f"Invalid etype '{v}'"
        return v

    @validator("data_points", pre=True, each_item=True)
    def instanciate_dps(cls, v, values):
        if "model_spec" in values and values["model_spec"]:
            # Convert `DataPointBase` instances back to dicts
            if isinstance(v, DataPointBase):
                v = v.dict()

            etype = v.get("etype")
            attr = v.get("attr")

            # Fetch datapoint model
            try:
                dp_model = values["model_spec"].attr(etype, attr).dp_model
            except (KeyError, TypeError) as e:
                raise ValueError(f"Attribute '{attr}' not found in entity '{etype}'") from e

            # Parse datapoint using model from attribute specification
            try:
                return dp_model.parse_obj(v)
            except ValidationError as e:
                raise ValueError(e) from e

        return v

    @validator("data_points", each_item=True)
    def validate_data_points(cls, v, values):
        if "etype" in values and "ekey" in values:
            assert (
                v.etype == values["etype"]
            ), f"Task's etype '{values['etype']}' doesn't match datapoint's etype '{v.etype}'"
            assert (
                v.eid == values["ekey"]
            ), f"Task's ekey '{values['ekey']}' doesn't match datapoint's eid '{v.eid}'"
        return v

    @root_validator
    def discard_attr_spec(cls, values):
        # This is run at the end of validation.
        # Discard attribute specification.
        del values["model_spec"]
        return values


class Snapshot(Task):
    """Snapshot

    Contains a list of linked entities for which a snapshot should be created.
    Attributes:
        entities: List of (entity_type, entity_id)
    """

    entities: list[tuple[str, str]]

    def routing_key(self):
        return "-".join(f"{etype}:{ekey}" for etype, ekey in self.entities)

    def as_message(self) -> str:
        return self.json()
