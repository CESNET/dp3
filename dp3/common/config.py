"""
NERDd - config file reader
"""
import os

import yaml
from pydantic import BaseModel, validator

from dp3.common.attrspec import AttrSpec, AttrSpecType
from dp3.common.base_attrs import BASE_ATTRIBS
from dp3.common.entityspec import EntitySpec


class NoDefault:
    pass


class MissingConfigError(Exception):
    pass


def hierarchical_get(self, key, default=NoDefault):
    """
    Return self[key] or "default" if key is not found. Allow hierarchical keys.

    Key may be a path (in dot notation) into a hierarchy of dicts. For example
      dictionary.get('abc.x.y')
    is equivalent to
      dictionary['abc']['x']['y']
    If some of the keys in the path is not present, default value is returned
    instead.
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


def hierarchical_update(self, other):
    """
    Update HierarchicalDict with other dictionary and merge common keys.

    If there is a key in both current and the other dictionary and values of
    both keys are dictionaries, they are merged together.
    Example:
      HierarchicalDict({'a': {'b': 1, 'c': 2}}).update({'a': {'b': 10, 'd': 3}})
      ->
      HierarchicalDict({'a': {'b': 10, 'c': 2, 'd': 3}})

    Changes the dictionary directly, returns None.
    """
    other = dict(other)
    for key in other.keys():
        if key in self:
            if isinstance(self[key], dict) and isinstance(other[key], dict):
                # The key is present in both dicts and both key values are dicts -> merge them
                hierarchical_update(self[key], other[key])
            else:
                # One of the key values is not a dict -> overwrite the value
                # in self by the one from other (like normal "update" does)
                self[key] = other[key]
        else:
            # key is not present in self -> set it to value from other
            self[key] = other[key]


class HierarchicalDict(dict):
    get = hierarchical_get
    update = hierarchical_update

    def __repr__(self):
        return f"HierarchicalDict({dict.__repr__(self)})"

    def copy(self):
        return HierarchicalDict(dict.copy(self))


def read_config(filepath: str) -> HierarchicalDict:
    """
    Read configuration file and return config as a dict-like object.

    The configuration file should contain a valid YAML
    - Comments may be included as lines starting with '#' (optionally preceded
      by whitespaces).

    This function reads the file and converts it to an dict-like object.
    The only difference from normal dict is its "get" method, which allows
    hierarchical keys (e.g. 'abc.x.y'). See doc of "hierarchical_get" function
    for more information.
    """
    with open(filepath) as file_content:
        return HierarchicalDict(yaml.safe_load(file_content))


def read_config_dir(dir_path: str, recursive: bool = False) -> HierarchicalDict:
    """
    Same as read_config but it loads whole configuration directory of YAML files,
    so only files ending with ".yml" are loaded.
    Each loaded configuration is located under key named after configuration filename.

    If recursive is set, then the configuration directory will be read recursively
    (including configuration files inside directories)
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


class EntitySpecDict(BaseModel):
    entity: EntitySpec
    attribs: dict[str, AttrSpecType]

    def __getitem__(self, item):
        return self.__getattribute__(item)

    @validator("entity", pre=True)
    def _parse_entity_spec(cls, v, values):
        return EntitySpec(v["id"], v)

    @validator("attribs", pre=True)
    def _parse_attr_spec(cls, v, values):
        assert isinstance(v, dict), "'attribs' must be a dictionary"
        return {attr_id: AttrSpec(attr_id, spec) for attr_id, spec in v.items()}

    @validator("attribs")
    def _add_base_attributes(cls, v, values):
        """Add base attributes - attributes that every entity_type should have"""
        v.update(BASE_ATTRIBS)
        return v


class ModelSpec(BaseModel):
    """
    Class representing the platform's current entity and attribute specification.
    """

    config: dict[str, EntitySpecDict]

    entities: dict[str, EntitySpec]  # entity id -> EntitySpec
    attributes: dict[tuple[str, str], AttrSpecType]  # (entity id, attribute id) -> AttrSpec
    entity_attributes: dict[str, dict[str, AttrSpecType]]  # entity id -> attribute id -> AttrSpec

    def __init__(self, config: HierarchicalDict):
        """
        Provided configuration must be a dict of following structure:
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

        Throws an exception if the specification is invalid
        """
        super().__init__(config=config, entities={}, attributes={}, entity_attributes={})

    @validator("entities")
    def _fill_entities(cls, v, values):
        return {
            entity_id: entity_dict["entity"] for entity_id, entity_dict in values["config"].items()
        }

    @validator("attributes")
    def _fill_attributes(cls, v, values):
        return {
            (entity_id, attr_id): attr_spec
            for entity_id, entity_dict in values["config"].items()
            for attr_id, attr_spec in entity_dict["attribs"].items()
        }

    @validator("entity_attributes")
    def _fill_entity_attributes(cls, v, values):
        return {
            entity_id: {attr_id: attr_spec for attr_id, attr_spec in entity_dict["attribs"].items()}
            for entity_id, entity_dict in values["config"].items()
        }

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
