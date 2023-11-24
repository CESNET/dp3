from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Optional

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_core.core_schema import FieldValidationInfo

from dp3.common.config import ModelSpec
from dp3.common.datapoint import DataPointBase


class Task(BaseModel, ABC):
    """
    A generic task type class.

    An abstraction for the [task queue][dp3.task_processing.task_queue] classes to depend upon.
    """

    @abstractmethod
    def routing_key(self) -> str:
        """
        Returns:
            A string to be used as a routing key between workers.
        """

    @abstractmethod
    def as_message(self) -> str:
        """
        Returns:
            A string representation of the object.
        """


def instanciate_dps(v, info: FieldValidationInfo):
    if "model_spec" in info.data and info.data["model_spec"]:
        # Convert `DataPointBase` instances back to dicts
        if isinstance(v, DataPointBase):
            v = v.model_dump()
        if isinstance(v, str):
            v = DataPointBase.model_validate(v)

        etype = v.get("etype")
        attr = v.get("attr")

        # Fetch datapoint model
        try:
            dp_model = info.data["model_spec"].attr(etype, attr).dp_model
        except (KeyError, TypeError) as e:
            raise ValueError(f"Attribute '{attr}' not found in entity '{etype}'") from e

        # Parse datapoint using model from attribute specification
        try:
            return dp_model.parse_obj(v)
        except ValidationError as e:
            raise ValueError(e) from e

    return v


def validate_data_points(v, info: FieldValidationInfo):
    if "etype" in info.data and "eid" in info.data:
        assert (
            v.etype == info.data["etype"]
        ), f"Task's etype '{info.data['etype']}' doesn't match datapoint's etype '{v.etype}'"
        assert (
            v.eid == info.data["eid"]
        ), f"Task's eid '{info.data['eid']}' doesn't match datapoint's eid '{v.eid}'"
    return v


def serialize_datapoint_json(dp: DataPointBase):
    return dp.model_dump_json()


ValidatedDataPoint = Annotated[
    DataPointBase,
    BeforeValidator(instanciate_dps),
    AfterValidator(validate_data_points),
]


class DataPointTask(Task):
    """DataPointTask

    Contains single task to be pushed to TaskQueue and processed.
    Attributes:
        etype: Entity type
        eid: Entity id / key
        data_points: List of DataPoints to process
        tags: List of tags
        ttl_tokens: Dictionary of TTL tokens.
        delete: If True, delete entity
    """

    # Model specification just for internal validation. Discarded after that.
    model_spec: ModelSpec

    etype: str
    eid: str
    data_points: list[ValidatedDataPoint] = []
    tags: list[Any] = []
    ttl_tokens: Optional[dict[str, datetime]] = None
    delete: bool = False

    def routing_key(self):
        return f"{self.etype}:{self.eid}"

    def as_message(self) -> str:
        return self.model_dump_json(exclude={"model_spec"})

    @field_validator("etype")
    def validate_etype(cls, v, info: FieldValidationInfo):
        if "model_spec" in info.data:
            assert v in info.data["model_spec"], f"Invalid etype '{v}'"
        return v

    @model_validator(mode="after")
    def discard_attr_spec(self):
        # This is run at the end of validation.
        # Discard attribute specification.
        del self.model_spec
        return self


class SnapshotMessageType(Enum):
    task = "task"
    linked_entities = "linked_entities"
    run_start = "run_start"
    run_end = "run_end"


class Snapshot(Task):
    """Snapshot

    Contains a list of entities, the meaning of which depends on the `type`.
    If `type` is "task", then the list contains linked entities for which a snapshot
    should be created. Otherwise `type` is "linked_entities", indicating which entities
    must be skipped in a parallelized creation of unlinked entities.

    Attributes:
        entities: List of (entity_type, entity_id)
        time: timestamp for snapshot creation
        final: If True, this is the last linked snapshot for the given time
    """

    entities: list[tuple[str, str]] = []
    time: datetime
    type: SnapshotMessageType
    final: bool = False

    def routing_key(self):
        return "-".join(f"{etype}:{eid}" for etype, eid in self.entities)

    def as_message(self) -> str:
        return self.json()
