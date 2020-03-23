import logging


class UnknownEntityType(ValueError):
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

    def get(self, etype, ekey):
        self._check_etype_support(etype)
        return self._db[etype].get(ekey, None)

    def get_attrib(self, etype, ekey, attrib):
        self._check_etype_support(etype)
        record = self.get(etype, ekey)
        return None if record is None else record.get(attrib)

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
        # record) such as :
        # https://stackoverflow.com/questions/4253960/sql-how-to-properly-check-if-a-record-exists
        return False if self.get(etype, ekey) is None else True

    def delete(self, etype, ekey):
        self._check_etype_support(etype)
        try:
            del self._db[ekey]
        except KeyError:
            pass

    def close(self):
        # close the connection
        pass

    def __del__(self):
        # destructor, close connection to database
        self.close()
