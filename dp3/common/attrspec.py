from datetime import timedelta
from enum import Flag
from typing import Any, Literal, Optional, Union

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    PrivateAttr,
    constr,
    create_model,
    validator,
)

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
from dp3.common.datatype import DataType
from dp3.common.entityspec import SpecModel
from dp3.common.utils import parse_time_duration

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

    max_age: Optional[timedelta] = None
    max_items: Optional[PositiveInt] = None
    expire_time: Optional[timedelta] = None
    pre_validity: Optional[timedelta] = timedelta()
    post_validity: Optional[timedelta] = timedelta()

    aggregate: bool = True

    @validator("max_age", "expire_time", "pre_validity", "post_validity", pre=True)
    def parse_time_duration(cls, v):
        if v:
            return parse_time_duration(v)

    @validator("expire_time", pre=True, always=True)
    def expire_time_inf_transform(cls, v):
        return None if v == "inf" else v


class TimeseriesTSParams(BaseModel):
    """Timeseries parameters field of timeseries attribute"""

    max_age: Optional[timedelta] = None
    time_step: Optional[timedelta] = None

    @validator("max_age", "time_step", pre=True)
    def parse_time_duration(cls, v):
        if v:
            return parse_time_duration(v)


class TimeseriesSeries(BaseModel):
    """Series of timeseries attribute"""

    data_type: DataType

    @validator("data_type")
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
    """

    id: constr(regex=ID_REGEX)
    name: str
    description: str = ""

    _dp_model = PrivateAttr()

    @property
    def dp_model(self) -> DataPointBase:
        return self._dp_model


class AttrSpecClassic(AttrSpecGeneric):
    """Parent of non-timeseries `AttrSpec` classes."""

    data_type: DataType
    editable: bool = False

    @property
    def is_relation(self) -> bool:
        """Returns whether specified attribute is a link."""
        return self.data_type.is_link

    @property
    def relation_to(self) -> str:
        """Returns linked entity id. Raises ValueError if attribute is not a link."""
        return self.data_type.get_linked_entity()


class AttrSpecPlain(AttrSpecClassic):
    """Plain attribute specification"""

    t = AttrType.PLAIN
    type: Literal["plain"] = Field(..., repr=False)

    def __init__(self, **data):
        super().__init__(**data)

        self._dp_model = create_model(
            f"DataPointPlain_{self.id}",
            __base__=DataPointPlainBase,
            v=(self.data_type.data_type, ...),
        )


class AttrSpecObservations(AttrSpecClassic):
    """Observations attribute specification"""

    t = AttrType.OBSERVATIONS
    type: Literal["observations"] = Field(..., repr=False)

    confidence: bool = False
    multi_value: bool = False
    history_params: ObservationsHistoryParams = Field(default_factory=ObservationsHistoryParams)
    history_force_graph: bool = False

    def __init__(self, **data):
        super().__init__(**data)

        value_validator = self.data_type.data_type

        self._dp_model = create_model(
            f"DataPointObservations_{self.id}",
            __base__=DataPointObservationsBase,
            v=(value_validator, ...),
        )


class AttrSpecTimeseries(AttrSpecGeneric):
    """Timeseries attribute specification"""

    t = AttrType.TIMESERIES
    type: Literal["timeseries"] = Field(..., repr=False)

    timeseries_type: Literal["regular", "irregular", "irregular_intervals"]
    series: dict[constr(regex=ID_REGEX), TimeseriesSeries] = {}
    timeseries_params: TimeseriesTSParams

    def __init__(self, **data):
        super().__init__(**data)

        # Typing of `v` field
        dp_value_typing = {}
        for s in self.series:
            data_type = self.series[s].data_type.data_type
            dp_value_typing[s] = ((list[data_type]), ...)

        # Validators
        dp_validators = {
            "v_validator": dp_ts_v_validator,
        }

        # Add root validator
        if self.timeseries_type == "regular":
            dp_validators["root_validator"] = dp_ts_root_validator_regular_wrapper(
                self.timeseries_params.time_step
            )
        elif self.timeseries_type == "irregular":
            dp_validators["root_validator"] = dp_ts_root_validator_irregular
        elif self.timeseries_type == "irregular_intervals":
            dp_validators["root_validator"] = dp_ts_root_validator_irregular_intervals

        self._dp_model = create_model(
            f"DataPointTimeseries_{self.id}",
            __base__=DataPointTimeseriesBase,
            __validators__=dp_validators,
            v=(create_model(f"DataPointTimeseriesValue_{self.id}", **dp_value_typing), ...),
        )

    @validator("series")
    def add_default_series(cls, v, values):
        ts_type = values["timeseries_type"]
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

    if "type" not in spec:
        raise ValueError("Missing mandatory attribute `type`")
    attr_type = AttrType.from_str(spec.get("type"))
    subclasses = {
        AttrType.PLAIN: AttrSpecPlain,
        AttrType.OBSERVATIONS: AttrSpecObservations,
        AttrType.TIMESERIES: AttrSpecTimeseries,
    }
    return subclasses[attr_type](id=id, **spec)
