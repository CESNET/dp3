"""
NERDd - config file reader
"""
import os
from typing import Union

import yaml

from dp3.common.attrspec import AttrSpec
from dp3.common.entityspec import EntitySpec
from .base_attrs import BASE_ATTRIBS


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
        while '.' in key:
            first_key, key = key.split('.', 1)
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
        return 'HierarchicalDict({})'.format(dict.__repr__(self))

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
    Same as read_config but it loads whole configuration directory of YAML files, so only files ending with ".yml" are
    loaded. Each loaded configuration is located under key named after configuration filename.

    If recursive is set, then the configuration directory will be read recursively (including configuration files
    inside directories)
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


# TODO: This should be moved elsewhere, this file should be generic, independent of entitiy/attribute config format
def load_attr_spec(config_in: HierarchicalDict) -> dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpec]]]]:
    """
    Load and validate entity/attribute specification
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
    config_out = {}

    err_msg_type = "Invalid configuration: type of '{}' is invalid (should be '{}', is '{}')"
    err_msg_missing_field = "Invalid configuration: mandatory field '{}' is missing"

    assert isinstance(config_in, dict), err_msg_type.format('config', 'dict')

    for entity_type, spec in config_in.items():
        # Validate config structure
        assert isinstance(spec, dict), err_msg_type.format(f'config[\"{entity_type}\"]', 'dict', type(spec))
        assert "entity" in spec, err_msg_missing_field.format('entity')
        assert "attribs" in spec, err_msg_missing_field.format('attribs')
        assert isinstance(spec["entity"], dict), err_msg_type.format('entity', 'dict', type(spec["entity"]))
        assert isinstance(spec["attribs"], dict), err_msg_type.format('attribs', 'dict', type(spec["attribs"]))

        config_out[entity_type] = {"entity": {}, "attribs": {}}

        # Init entity specification
        try:
            config_out[entity_type]["entity"] = EntitySpec(entity_type, spec["entity"])
        except Exception as e:
            raise AssertionError(f"Invalid specification of entity {entity_type}: {e}")

        # Init attribute specification
        for attr in spec["attribs"]:
            try:
                config_out[entity_type]["attribs"][attr] = AttrSpec(attr, spec["attribs"][attr])
            except Exception as e:
                raise AssertionError(f"Invalid specification of attribute '{attr}': {e}")

        # Add base attributes - attributes that every entity_type should have
        config_out[entity_type]["attribs"].update(BASE_ATTRIBS)

    return config_out


