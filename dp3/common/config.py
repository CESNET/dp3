"""
Platform config file reader and config model.
"""
import os
from typing import Optional

import yaml
from pydantic import BaseModel, Extra, Field, NonNegativeInt, PositiveInt, root_validator, validator

from dp3.common.attrspec import AttrSpec, AttrSpecClassic, AttrSpecGeneric, AttrSpecType
from dp3.common.entityspec import EntitySpec


class NoDefault:
    pass


class MissingConfigError(Exception):
    pass


class HierarchicalDict(dict):
    """Extension of built-in `dict` that simplifies working with a nested hierarchy of dicts."""

    def __repr__(self):
        return f"HierarchicalDict({dict.__repr__(self)})"

    def copy(self):
        return HierarchicalDict(dict.copy(self))

    def get(self, key, default=NoDefault):
        """
        Key may be a path (in dot notation) into a hierarchy of dicts. For example
          `dictionary.get('abc.x.y')`
        is equivalent to
          `dictionary['abc']['x']['y']`.

        :returns: `self[key]` or `default` if key is not found.
        """
        d = self
        try:
            while "." in key:
                first_key, key = key.split(".", 1)
                d = d[first_key]
            return d[key]
        except (KeyError, TypeError):
            pass  # not found - continue below
        if default is NoDefault:
            raise MissingConfigError("Mandatory configuration element is missing: " + key)
        else:
            return default

    def update(self, other, **kwargs):
        """
        Update `HierarchicalDict` with other dictionary and merge common keys.

        If there is a key in both current and the other dictionary and values of
        both keys are dictionaries, they are merged together.

        Example:
        ```
        HierarchicalDict({'a': {'b': 1, 'c': 2}}).update({'a': {'b': 10, 'd': 3}})
        ->
        HierarchicalDict({'a': {'b': 10, 'c': 2, 'd': 3}})
        ```
        Changes the dictionary directly, returns `None`.
        """
        other = dict(other)
        for key in other:
            if key in self:
                if isinstance(self[key], dict) and isinstance(other[key], dict):
                    # The key is present in both dicts and both key values are dicts -> merge them
                    HierarchicalDict.update(self[key], other[key])
                else:
                    # One of the key values is not a dict -> overwrite the value
                    # in self by the one from other (like normal "update" does)
                    self[key] = other[key]
            else:
                # key is not present in self -> set it to value from other
                self[key] = other[key]


def read_config(filepath: str) -> HierarchicalDict:
    """
    Read configuration file and return config as a dict-like object.

    The configuration file should contain a valid YAML
    - Comments may be included as lines starting with `#` (optionally preceded
      by whitespaces).

    This function reads the file and converts it to a `HierarchicalDict`.
    The only difference from built-in `dict` is its `get` method, which allows
    hierarchical keys (e.g. `abc.x.y`).
    See [doc of get method][dp3.common.config.HierarchicalDict.get] for more information.
    """
    with open(filepath) as file_content:
        return HierarchicalDict(yaml.safe_load(file_content))


def read_config_dir(dir_path: str, recursive: bool = False) -> HierarchicalDict:
    """
    Same as [read_config][dp3.common.config.read_config],
    but it loads whole configuration directory of YAML files,
    so only files ending with ".yml" are loaded.
    Each loaded configuration is located under key named after configuration filename.

    Args:
        dir_path: Path to read config from.
        recursive: If `recursive` is set, then the configuration directory will be read
            recursively (including configuration files inside directories).
    """
    all_files_paths = os.listdir(dir_path)
    config = HierarchicalDict()
    for config_filename in all_files_paths:
        config_full_path = os.path.join(dir_path, config_filename)
        if os.path.isdir(config_full_path) and recursive:
            loaded_config = read_config_dir(config_full_path, recursive)
        elif os.path.isfile(config_full_path) and config_filename.endswith(".yml"):
            try:
                loaded_config = read_config(config_full_path)
            except TypeError:
                # configuration file is empty
                continue
            # remove '.yml' suffix of filename
            config_filename = config_filename[:-4]
        else:
            continue
        # place configuration files into another dictionary level named by config dictionary name
        config[config_filename] = loaded_config
    return config


class CronExpression(BaseModel, extra=Extra.forbid):
    """
    Cron expression used for scheduling. Also support standard cron expressions, such as

    - "*/15" (every 15 units)
    - "1,2,3" (1, 2 and 3)
    - "1-3" (1, 2 and 3)

    Attributes:
        year: 4-digit year
        month: month (1-12)
        day: day of month (1-31)
        week: ISO week (1-53)
        day_of_week: number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
        hour: hour (0-23)
        minute: minute (0-59)
        second: second (0-59)
        timezone: Timezone for time specification (default is UTC).
    """

    second: Optional[str] = Field(default=None, regex=r"^((\d+,)+\d+|(\d+-\d+)|\d+|(\*\/\d+)|\*)$")
    minute: Optional[str] = Field(default=None, regex=r"^((\d+,)+\d+|(\d+-\d+)|\d+|(\*\/\d+)|\*)$")
    hour: Optional[str] = Field(default=None, regex=r"^((\d+,)+\d+|(\d+-\d+)|\d+|(\*\/\d+)|\*)$")

    day: Optional[str] = Field(default=None, regex=r"^((\d+,)+\d+|(\d+-\d+)|\d+|(\*\/\d+)|\*)$")
    day_of_week: Optional[str] = Field(default=None, regex=r"^(\d|mon|tue|wed|thu|fri|sat|sun)$")

    week: Optional[int] = Field(default=None, ge=1, le=53)
    month: Optional[int] = Field(default=None, ge=1, le=12)
    year: Optional[str] = Field(default=None, regex=r"^\d{4}$")

    timezone: str = "UTC"


class EntitySpecDict(BaseModel):
    """Class representing full specification of an entity.

    Attributes:
        entity: Specification and settings of entity itself.
        attribs: A mapping of attribute id -> AttrSpec
    """

    entity: EntitySpec
    attribs: dict[str, AttrSpecType]

    def __getitem__(self, item):
        return self.__getattribute__(item)

    @validator("entity", pre=True)
    def _parse_entity_spec(cls, v, values):
        if isinstance(v, EntitySpec):
            return v
        return EntitySpec.parse_obj(v)

    @validator("attribs", pre=True)
    def _parse_attr_spec(cls, v, values):
        assert isinstance(v, dict), "'attribs' must be a dictionary"
        return {
            attr_id: AttrSpec(attr_id, spec) if not isinstance(spec, AttrSpecGeneric) else spec
            for attr_id, spec in v.items()
        }


class ModelSpec(BaseModel):
    """
    Class representing the platform's current entity and attribute specification.

    Attributes:
        config: Legacy config format, exactly mirrors the config files.

        entities: Mapping of entity id -> EntitySpec
        attributes: Mapping of (entity id, attribute id) -> AttrSpec
        entity_attributes: Mapping of entity id -> attribute id -> AttrSpec

        relations: Mapping of (entity id, attribute id) -> AttrSpec
            only contains attributes which are relations.
    """

    config: dict[str, EntitySpecDict]

    entities: dict[str, EntitySpec]
    attributes: dict[tuple[str, str], AttrSpecType]
    entity_attributes: dict[str, dict[str, AttrSpecType]]

    relations: dict[tuple[str, str], AttrSpecType]

    def __init__(self, config: HierarchicalDict):
        """
        Provided configuration must be a dict of following structure:
        ```
        {
            <entity type>: {
                'entity': {
                    entity specification
                },
                'attribs': {
                    <attr id>: {
                        attribute specification
                    },
                    other attributes
                }
            },
            other entity types
        }
        ```
        Raises:
            ValueError: if the specification is invalid.
        """
        super().__init__(
            config=config, entities={}, attributes={}, entity_attributes={}, relations={}
        )

    @validator("entities")
    def _fill_entities(cls, v, values):
        if "config" not in values:
            return v
        return {
            entity_id: entity_dict["entity"] for entity_id, entity_dict in values["config"].items()
        }

    @validator("attributes")
    def _fill_attributes(cls, v, values):
        if "config" not in values:
            return v
        return {
            (entity_id, attr_id): attr_spec
            for entity_id, entity_dict in values["config"].items()
            for attr_id, attr_spec in entity_dict["attribs"].items()
        }

    @validator("entity_attributes")
    def _fill_entity_attributes(cls, v, values):
        if "config" not in values:
            return v
        return {
            entity_id: dict(entity_dict["attribs"].items())
            for entity_id, entity_dict in values["config"].items()
        }

    @validator("relations")
    def _fill_relations(cls, v, values):
        if "attributes" not in values:
            return v
        return {
            entity_id_attr_id: attr_spec
            for entity_id_attr_id, attr_spec in values["attributes"].items()
            if isinstance(attr_spec, AttrSpecClassic) and attr_spec.is_relation
        }

    @root_validator
    def _validate_relations(cls, values):
        """Validate that relation type attributes link to existing entities."""
        for entity_attr, attr_spec in values["relations"].items():
            if attr_spec.relation_to not in values["entities"]:
                entity, attr = entity_attr
                raise ValueError(
                    f"'{attr_spec.relation_to}', linked by '{attr}' is not a valid entity."
                )
        return values

    def attr(self, entity_type: str, attr: str) -> AttrSpecType:
        return self.attributes[entity_type, attr]

    def attribs(self, entity_type: str) -> dict[str, AttrSpecType]:
        return self.entity_attributes[entity_type]

    def entity(self, entity_type: str) -> EntitySpec:
        return self.entities[entity_type]

    def items(self):
        return self.config.items()

    def keys(self):
        return self.config.keys()

    def __contains__(self, item):
        return item in self.config

    def __getitem__(self, item):
        return self.config[item]

    def __setitem__(self, key, value):
        self.config[key] = value


class PlatformConfig(BaseModel):
    """
    An aggregation of configuration available to modules.

    Attributes:
        app_name: Name of the application, used when naming various structures of the platform
        config_base_path: Path to directory containing platform config
        config: A dictionary that contains the platform config
        model_spec: Specification of the platform's model (entities and attributes)

        num_processes: Number of worker processes
        process_index: Index of current process
    """

    app_name: str
    config_base_path: str
    config: HierarchicalDict
    model_spec: ModelSpec

    num_processes: PositiveInt
    process_index: NonNegativeInt

    @validator("process_index")
    def valid_process_index(cls, v, values):
        if "num_processes" not in values:
            return v

        assert (
            v < values["num_processes"]
        ), "Process index must be less than total number of processes"
        return v
