import logging


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

    TODO add support for history data points
    """
    # List of known/supported entity types - currently only IP addresses (both IPv4 and IPv6 are treated the same)
    _SUPPORTED_TYPES = ['ip']

    def __init__(self, config):
        self._db = {}
        self._log = logging.getLogger("EntityDatabase")
        # init all entity structures
        for etype in EntityDatabase._SUPPORTED_TYPES:
            self._db[etype] = {}

    def _check_etype_support(self, etype):
        if etype not in EntityDatabase._SUPPORTED_TYPES:
            raise UnknownEntityType("There is no collection for entity type " + str(etype))

    def get_record(self, etype, ekey):
        self._check_etype_support(etype)
        return self._db[etype].get(ekey, None)

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

    def update(self, etype, ekey, updates):
        self._check_etype_support(etype)
        # updates are passed in form of dictionary
        try:
            # record should be always created before update, but to be sure, it is in try block
            self._db[etype][ekey].update(updates)
        except KeyError:
            self._db[etype][ekey] = updates

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
            del self._db[etype][ekey]
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
            del self._db[etype][ekey][attib_name]
        except KeyError:
            pass

    def close(self):
        # close the connection
        pass

    def __del__(self):
        # destructor, close connection to database
        self.close()
