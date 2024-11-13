import hashlib
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from typing import Annotated, Any, Callable, Optional, Union

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    Discriminator,
    PlainSerializer,
    Tag,
    TypeAdapter,
    ValidationError,
    field_validator,
)
from pydantic_core.core_schema import FieldValidationInfo

from dp3.common.config import ModelSpec, entity_type_context, get_entity_type_context
from dp3.common.datapoint import DataPointBase, to_json_friendly
from dp3.common.datatype import AnyEidT
from dp3.common.mac_address import MACAddress

_init_context_var = ContextVar("_init_context_var", default=None)


def HASH(key: str) -> int:
    """Hash function used to distribute tasks to worker processes.
    Args:
        key: to be hashed
    Returns:
        last 4 bytes of MD5
    """
    return int(hashlib.md5(key.encode("utf8")).hexdigest()[-4:], 16)


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

    def hashed_routing_key(self) -> int:
        """
        Returns:
            An integer to be used as a hashed routing key between workers.
        """
        return HASH(self.routing_key())

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
    assert context is not None, "Missing context"
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
    if "etype" not in info.data or "eid" not in info.data:
        return v
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
    eid: Annotated[Any, PlainSerializer(to_json_friendly, when_used="json")]
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

    @field_validator("eid")
    def validate_eid(cls, v, info: FieldValidationInfo):
        if "etype" not in info.data:
            return v

        context = info.context
        if context and "model_spec" in context:
            ms: ModelSpec = context["model_spec"]
            return ms.entities[info.data["etype"]].validate_eid(v)
        else:
            raise AssertionError("Missing `model_spec` in context")


def parse_data_point_task(task: str, model_spec: ModelSpec) -> DataPointTask:
    with task_context(model_spec):
        return DataPointTask.model_validate_json(task)


class SnapshotMessageType(Enum):
    task = "task"
    linked_entities = "linked_entities"
    run_start = "run_start"
    run_end = "run_end"


def get_discriminator_value(entity_tuple: tuple[str, Any]) -> str:
    name_2_type_name = get_entity_type_context()
    return name_2_type_name[entity_tuple[0]]


EntityTuple = Annotated[
    Union[
        Annotated[tuple[str, str], Tag("string")],
        Annotated[tuple[str, int], Tag("int")],
        Annotated[tuple[str, IPv4Address], Tag("ipv4")],
        Annotated[tuple[str, IPv6Address], Tag("ipv6")],
        Annotated[tuple[str, MACAddress], Tag("mac")],
    ],
    Discriminator(get_discriminator_value),
]


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

    entities: list[EntityTuple] = []
    time: datetime
    type: SnapshotMessageType
    final: bool = False

    def routing_key(self):
        return "-".join(f"{etype}:{eid}" for etype, eid in self.entities)

    def as_message(self) -> str:
        return self.model_dump_json()

    @staticmethod
    def get_validator(model_spec: ModelSpec) -> Callable[[Union[str, bytes]], "Snapshot"]:
        def json_validator(serialized: Union[str, bytes]) -> Snapshot:
            with entity_type_context(model_spec):
                return Snapshot.model_validate_json(serialized)

        return json_validator


EntityTuplesAdapter = TypeAdapter(list[EntityTuple])


def validate_entities(model_spec: ModelSpec, v: list[tuple[str, str]]) -> list[EntityTuple]:
    with entity_type_context(model_spec):
        return EntityTuplesAdapter.validate_python(v)


def parse_eids_from_cache(model_spec: ModelSpec, link_entity_entries: list[str]) -> list[AnyEidT]:
    """Parses entity IDs from the "Link" cache.

    Args:
        model_spec: Model specification.
        link_entity_entries: List of entity entries from the cache.

    Returns:
        List of entity IDs.
    """
    raw = [entry.split("#", maxsplit=1) for entry in link_entity_entries]
    return [eid for etype, eid in validate_entities(model_spec, raw)]


def parse_eid_tuples_from_cache(
    model_spec: ModelSpec, link_entity_entries: list[str]
) -> list[tuple[str, AnyEidT]]:
    """Parses entity IDs from the "Link" cache.

    Args:
        model_spec: Model specification.
        link_entity_entries: List of entity entries from the cache.

    Returns:
        List of tuples (entity type, entity ID).
    """
    raw = [entry.split("#", maxsplit=1) for entry in link_entity_entries]
    return validate_entities(model_spec, raw)
