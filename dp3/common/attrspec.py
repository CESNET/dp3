from datetime import timedelta
from enum import Flag
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    PrivateAttr,
    create_model,
    field_validator,
    model_validator,
)
from pydantic_core.core_schema import FieldValidationInfo

from dp3.common.context import get_entity_context
from dp3.common.datapoint import (
    DataPointBase,
    DataPointObservationsBase,
    DataPointPlainBase,
    DataPointTimeseriesBase,
    dp_ts_root_validator_irregular,
    dp_ts_root_validator_irregular_intervals,
    dp_ts_root_validator_regular_wrapper,
    dp_ts_v_validator,
)
from dp3.common.datatype import DataType, ReadOnly
from dp3.common.entityspec import SpecModel
from dp3.common.types import ParsedTimedelta

# Regex of attribute and series id's
ID_REGEX = r"^[a-zA-Z_][a-zA-Z0-9_-]*$"

# Dict of timeseries type spec
timeseries_types = {
    "regular": {"default_series": {}, "sort_by": "t1"},
    "irregular": {"default_series": {"time": {"data_type": "time"}}, "sort_by": "time"},
    "irregular_intervals": {
        "default_series": {"time_first": {"data_type": "time"}, "time_last": {"data_type": "time"}},
        "sort_by": "time_first",
    },
}


class AttrType(Flag):
    """Enum of attribute types

    `PLAIN` = 1
    `OBSERVATIONS` = 2
    `TIMESERIES` = 4
    """

    PLAIN = 1
    OBSERVATIONS = 2
    TIMESERIES = 4

    @classmethod
    def from_str(cls, type_str: str):
        """
        Convert string representation like "plain" to AttrType.
        """
        try:
            return cls(cls[type_str.upper()])
        except Exception as e:
            raise ValueError(f"Invalid `type` of attribute '{type_str}'") from e


class ObservationsHistoryParams(BaseModel):
    """History parameters field of observations attribute"""

    max_age: Optional[ParsedTimedelta] = None
    max_items: Optional[PositiveInt] = None
    expire_time: Optional[ParsedTimedelta] = None
    pre_validity: Optional[ParsedTimedelta] = timedelta()
    post_validity: Optional[ParsedTimedelta] = timedelta()

    aggregate: bool = True

    @field_validator("expire_time", mode="before")
    @classmethod
    def expire_time_inf_transform(cls, v):
        return None if v == "inf" else v


class TimeseriesTSParams(BaseModel):
    """Timeseries parameters field of timeseries attribute"""

    max_age: Optional[ParsedTimedelta] = None
    time_step: Optional[ParsedTimedelta] = None


class TimeseriesSeries(BaseModel):
    """Series of timeseries attribute"""

    data_type: DataType

    @field_validator("data_type")
    @classmethod
    def check_series_data_type(cls, v):
        assert str(v) in [
            "int",
            "int64",
            "float",
            "time",
        ], f"Data type of series must be one of int, int64, float, time; not {v}"
        return v


class AttrSpecGeneric(SpecModel, use_enum_values=True):
    """Base of attribute specification

    Parent of other `AttrSpec` classes.

    Attributes:
        ttl: Optional extension of TTL of the entity
            - will be ignored if lifetime setting does not match.
    """

    id: str = Field(pattern=ID_REGEX)
    name: str
    description: str = ""
    ttl: Optional[ParsedTimedelta] = timedelta()

    _dp_model = PrivateAttr()

    @property
    def dp_model(self) -> DataPointBase:
        return self._dp_model


class AttrSpecClassic(AttrSpecGeneric):
    """Parent of non-timeseries `AttrSpec` classes."""

    data_type: DataType
    editable: bool = False

    @property
    def is_iterable(self) -> bool:
        """Returns whether specified attribute is iterable."""
        return self.data_type.iterable

    @property
    def element_type(self) -> DataType:
        """Returns the element type for iterable data types."""
        return self.data_type.elem_type

    @property
    def is_relation(self) -> bool:
        """Returns whether specified attribute is a link."""
        return self.data_type.is_link or (
            self.data_type.iterable and self.data_type.elem_type.is_link
        )

    @property
    def relation_to(self) -> str:
        """Returns linked entity id. Raises ValueError if attribute is not a link."""
        if self.data_type.iterable:
            return self.element_type.get_linked_entity()
        return self.data_type.get_linked_entity()

    @property
    def is_mirrored(self) -> bool:
        """Returns whether specified attribute is a mirrored link."""
        if self.data_type.iterable:
            return self.element_type.mirror_link
        return self.data_type.mirror_link

    @property
    def mirror_as(self) -> str:
        """
        Returns:
             name of the mirrored attribute.
        Raises:
            ValueError: If attribute is not a mirrored link.
        """
        if self.data_type.iterable:
            return self.element_type.mirror_as
        return self.data_type.mirror_as


class AttrSpecPlain(AttrSpecClassic):
    """Plain attribute specification"""

    t: AttrType = AttrType.PLAIN
    type: Literal["plain"] = Field(..., repr=False)

    def __init__(self, **data):
        super().__init__(**data)

        entity_spec = get_entity_context()["self"]
        self._dp_model = create_model(
            f"DataPointPlain_{self.id}",
            __base__=DataPointPlainBase,
            eid=(entity_spec.id_data_type.data_type, ...),
            v=(self.data_type.data_type, ...),
        )


class AttrSpecReadOnly(AttrSpecPlain):
    """Read-only plain attribute specification. Used for internal attributes."""

    def __init__(self, **data):
        super().__init__(**data)

        entity_spec = get_entity_context()["self"]
        self._dp_model = create_model(
            f"DataPointReadOnly_{self.id}",
            __base__=DataPointPlainBase,
            eid=(entity_spec.id_data_type.data_type, ...),
            v=(ReadOnly, ...),
        )


class AttrSpecObservations(AttrSpecClassic):
    """Observations attribute specification"""

    t: AttrType = AttrType.OBSERVATIONS
    type: Literal["observations"] = Field(..., repr=False)

    confidence: bool = False
    multi_value: bool = False
    history_params: ObservationsHistoryParams = Field(default_factory=ObservationsHistoryParams)
    history_force_graph: bool = False

    def __init__(self, **data):
        super().__init__(**data)

        value_validator = self.data_type.data_type
        entity_spec = get_entity_context()["self"]
        self._dp_model = create_model(
            f"DataPointObservations_{self.id}",
            __base__=DataPointObservationsBase,
            eid=(entity_spec.id_data_type.data_type, ...),
            v=(value_validator, ...),
        )


class AttrSpecTimeseries(AttrSpecGeneric):
    """Timeseries attribute specification"""

    t: AttrType = AttrType.TIMESERIES
    type: Literal["timeseries"] = Field(..., repr=False)

    timeseries_type: Literal["regular", "irregular", "irregular_intervals"]
    series: dict[Annotated[str, Field(pattern=ID_REGEX)], TimeseriesSeries] = {}
    timeseries_params: TimeseriesTSParams

    def __init__(self, **data):
        super().__init__(**data)

        entity_spec = get_entity_context()["self"]

        # Typing of `v` field
        dp_value_typing = {}
        for s in self.series:
            data_type = self.series[s].data_type.data_type
            dp_value_typing[s] = ((list[data_type]), ...)

        # Add root validator
        if self.timeseries_type == "regular":
            root_validator = dp_ts_root_validator_regular_wrapper(self.timeseries_params.time_step)
        elif self.timeseries_type == "irregular":
            root_validator = dp_ts_root_validator_irregular
        elif self.timeseries_type == "irregular_intervals":
            root_validator = dp_ts_root_validator_irregular_intervals
        else:
            raise ValueError(f"Unknown timeseries type '{self.timeseries_type}'")

        # Validators
        dp_validators = {
            "v_validator": field_validator("v")(dp_ts_v_validator),
            "root_validator": model_validator(mode="after")(root_validator),
        }

        self._dp_model = create_model(
            f"DataPointTimeseries_{self.id}",
            __base__=DataPointTimeseriesBase,
            __validators__=dp_validators,
            eid=(entity_spec.id_data_type.data_type, ...),
            v=(create_model(f"DataPointTimeseriesValue_{self.id}", **dp_value_typing), ...),
        )

    @field_validator("series")
    @classmethod
    def add_default_series(cls, v, info: FieldValidationInfo):
        ts_type = info.data["timeseries_type"]
        default_series = timeseries_types[ts_type]["default_series"]

        for s in default_series:
            v[s] = TimeseriesSeries(**default_series[s])

        return v


"""A type union that covers AttrSpec class types:

- [AttrSpecPlain][dp3.common.attrspec.AttrSpecPlain]
- [AttrSpecObservations][dp3.common.attrspec.AttrSpecObservations]
- [AttrSpecTimeseries][dp3.common.attrspec.AttrSpecTimeseries]
"""
AttrSpecType = Union[AttrSpecTimeseries, AttrSpecObservations, AttrSpecPlain]


def AttrSpec(id: str, spec: dict[str, Any]) -> AttrSpecType:
    """Factory for `AttrSpec` classes"""

    assert isinstance(spec, dict), "Attribute specification must be a dict"
    if "type" not in spec:
        raise ValueError("Missing mandatory attribute `type`")
    attr_type = AttrType.from_str(spec.get("type"))
    subclasses = {
        AttrType.PLAIN: AttrSpecPlain,
        AttrType.OBSERVATIONS: AttrSpecObservations,
        AttrType.TIMESERIES: AttrSpecTimeseries,
    }

    spec["id"] = id  # Fill in the id if not present, else overwrite
    return subclasses[attr_type](**spec)
