from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Optional

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ValidationError,
    field_validator,
)
from pydantic_core.core_schema import FieldValidationInfo

from dp3.common.config import ModelSpec
from dp3.common.datapoint import DataPointBase

_init_context_var = ContextVar("_init_context_var", default=None)


@contextmanager
def task_context(model_spec: ModelSpec) -> Iterator[None]:
    """Context manager for setting the `model_spec` context variable."""
    token = _init_context_var.set({"model_spec": model_spec})
    try:
        yield
    finally:
        _init_context_var.reset(token)


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
    # If already instantiated, return
    if isinstance(v, DataPointBase):
        return v

    etype = v.get("etype")
    attr = v.get("attr")

    # Fetch datapoint model
    context = info.context
    assert context.get("model_spec"), "Missing `model_spec` in context"
    try:
        dp_model = context.get("model_spec").attr(etype, attr).dp_model
    except (KeyError, TypeError) as e:
        raise ValueError(f"Attribute '{attr}' not found in entity '{etype}'") from e

    # Parse datapoint using model from attribute specification
    try:
        return dp_model.model_validate(v)
    except ValidationError as e:
        raise ValueError(e) from e


def validate_data_points(v, info: FieldValidationInfo):
    assert (
        v.etype == info.data["etype"]
    ), f"Task's etype '{info.data['etype']}' doesn't match datapoint's etype '{v.etype}'"
    assert (
        v.eid == info.data["eid"]
    ), f"Task's eid '{info.data['eid']}' doesn't match datapoint's eid '{v.eid}'"
    return v


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

    etype: str
    eid: str
    data_points: list[ValidatedDataPoint] = []
    tags: list[Any] = []
    ttl_tokens: Optional[dict[str, datetime]] = None
    delete: bool = False

    def __init__(__pydantic_self__, **data: Any) -> None:
        """Set the `model_spec` context variable and initialize the Task.

        See https://docs.pydantic.dev/latest/concepts/validators/#validation-context
        """
        __pydantic_self__.__pydantic_validator__.validate_python(
            data,
            self_instance=__pydantic_self__,
            context=_init_context_var.get(),
        )

    def routing_key(self):
        return f"{self.etype}:{self.eid}"

    def as_message(self) -> str:
        return self.model_dump_json(exclude_defaults=True, exclude_unset=True)

    @field_validator("etype")
    def validate_etype(cls, v, info: FieldValidationInfo):
        context = info.context
        if context and "model_spec" in context:
            assert v in context["model_spec"], f"Invalid etype '{v}'"
        else:
            raise AssertionError("Missing `model_spec` in context")
        return v


def parse_data_point_task(task: str, model_spec: ModelSpec) -> DataPointTask:
    with task_context(model_spec):
        return DataPointTask.model_validate_json(task)


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
        return self.model_dump_json()
