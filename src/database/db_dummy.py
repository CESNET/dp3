import logging
from collections import defaultdict


class UnknownEntityType(ValueError):
    pass


class AttributeValueNotSet(ValueError):
    pass


class EntityDatabase:
    """
    Abstract layer above the entity database. It provides an interface for
    database operations independet on underlying database system.

    This is a trivial in-memory database based on Python dict - useful only for
    debugging/testing.
    """
    # List of known/supported entity types - currently only IP addresses (both IPv4 and IPv6 are treated the same)
    _SUPPORTED_TYPES = ['ip']

    def __init__(self, config):
        self._db_entities = {}
        self._db_data_points = {}
        self._log = logging.getLogger("EntityDatabase")
        # init all entity structures
        for etype in EntityDatabase._SUPPORTED_TYPES:
            self._db_entities[etype] = {}
            # must be saved as list, because there may be multiple data points under same id (key)
            self._db_data_points[etype] = defaultdict(list)

    def _check_etype_support(self, etype):
        if etype not in EntityDatabase._SUPPORTED_TYPES:
            raise UnknownEntityType("There is no collection for entity type " + str(etype))

    def get_record(self, etype, ekey):
        self._check_etype_support(etype)
        return self._db_entities[etype].get(ekey, None)

    def get_attrib(self, etype, ekey, attrib):
        """
        Returns attribute's value of database record.
        :param etype: type of entity
        :param ekey: entity id
        :param attrib: attribute's name
        :return: attribute's value
        :raise AttributeValueNotSet when the attribute has no value set yet
        """
        self._check_etype_support(etype)
        record = self.get_record(etype, ekey)
        try:
            return record[attrib]
        except KeyError:
            raise AttributeValueNotSet(f"Attribute {attrib} is not set!")

    def create_new_record(self, etype, ekey, body=None):
        self._check_etype_support(etype)
        self._db_entities[etype][ekey] = {'id': ekey} if body is None else body

    def create_new_data_point(self, etype, attr_name, data_point):
        """
        Creates new data point in database
        :param etype: entity type (e.g. 'ip')
        :param attr_name: name of attribute, to which the data point corresponds
        :param data_point: body of data point
        :return: None
        """
        self._check_etype_support(etype)
        self._db_data_points[etype][attr_name].append(data_point)

    def update(self, etype, ekey, updates):
        self._check_etype_support(etype)
        # updates are passed in form of dictionary
        try:
            self._db_entities[etype][ekey].update(updates)
        except KeyError:
            # when new record gets created
            self._db_entities[etype][ekey] = updates

    def exists(self, etype, ekey):
        self._check_etype_support(etype)
        # In real Database Manager class should be implemented with better performance (not just simply get whole
        # record) such as:
        # https://stackoverflow.com/questions/4253960/sql-how-to-properly-check-if-a-record-exists
        return False if self.get_record(etype, ekey) is None else True

    def delete_record(self, etype, ekey):
        """
        Delete whole record.
        :param etype: entity type
        :param ekey: id of record, which should be deleted
        :return: None
        """
        self._check_etype_support(etype)
        try:
            del self._db_entities[etype][ekey]
        except KeyError:
            pass

    def delete_attribute(self, etype, ekey, attib_name):
        """
        Delete attribute from record.
        :param etype: entity type
        :param ekey: id of record
        :param attib_name: name of attribute, which should be deleted
        :return: None
        """
        try:
            del self._db_entities[etype][ekey][attib_name]
        except KeyError:
            pass

    def close(self):
        # close the connection
        pass

    def __del__(self):
        # destructor, close connection to database
        self.close()
