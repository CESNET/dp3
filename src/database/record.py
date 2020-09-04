import logging
from datetime import datetime

from .db_dummy import AttributeValueNotSet
from .database import EntityDatabase


class Record:
    """
    Internal cache for database record and its updates to prevent loading whole record from database.
    Only record's attributes, which are really needed, will be loaded.
    """
    # TODO remove EntityDatabase
    def __init__(self, db: EntityDatabase, table_name, key):
        self._db_connection = db

        self._log = logging.getLogger("Record")

        self.table_name = table_name
        self.key = key

        # record structure
        self._record = {}
        # structure, which will hold all attributes, which were updated, all their updated values
        self._record_changes = {}
        self.exists_in_db = self.exists()

    def _load_from_db(self, attrib_name):
        """
        Loads attribute from database into cache.
        WARNING: If the attribute has no value set in database yet, the cache remains the same
        :param attrib_name: attribute's name
        :return: None
        """
        try:
            self._record[attrib_name] = self._db_connection.get_attrib(self.table_name, self.key, attrib_name)
        except AttributeValueNotSet: # TODO: the EntityDatabase.get_attrib doesn't raise exception like this
            pass

    def __getitem__(self, attrib_name):
        """
        Overrides functionality, when "Record[key]" is called. Gets value from record.
        :param attrib_name: key to value, which wants to be obtained from record
        :return: value, which is saved under the key in record
        :raise KeyError when the attrib_name does not exist in database under in the record
        """
        value = self._record.get(attrib_name)
        # if attribute is not loaded into cache yet, load it from database
        if value is None:
            self._load_from_db(attrib_name)

        return self._record[attrib_name]

    def __setitem__(self, attrib_name, value):
        """
        Overrides functionality, when "Record[key] = value" is called. Sets value in record.
        :param attrib_name: attribute's name in record
        :param value: new value under the key
        :return: None
        """
        self._record[attrib_name] = value
        # cache the changes
        self._record_changes[attrib_name] = value

    def __contains__(self, attrib_name):
        """
        Overrides functionality, when "key in Record" is called.
        :param attrib_name: key name
        :return: True if attribute is in record, False otherwise
        """
        if attrib_name in self._record:
            return True
        # if attribute not in cache, try to load it from database (if does not exist in database, cache will not be
        # updated)
        self._load_from_db(attrib_name)
        return attrib_name in self._record

    def __delitem__(self, attrib_name):
        """
        Overrides functionality, when "del Record[key]" is called. Deletes attribute from record.
        :param attrib_name: attribute's name, which should be deleted
        :return: None
        """
        try:
            del self._record[attrib_name]
        except KeyError:
            pass
        self._db_connection.delete_attribute(self.table_name, self.key, attrib_name)

    def exists(self):
        """
        Checks, whether record exists in the database.
        :return: True if exists record exists in the database, False otherwise
        """
        return self._db_connection.exists(self.table_name, self.key)

    def update(self, dict_update):
        """
        Behaves like classic dict.update()
        """
        self._record.update(dict_update)
        self._record_changes.update(dict_update)

    def get(self, attrib_name, default_val=None, load_from_db=True):
        """
        Behaves same as Dict's get, but can request loading record from database with 'load_from_db' flag, if not cached
        """
        value = self._record.get(attrib_name)
        if value is None and load_from_db:
            self._load_from_db(attrib_name)

        return self._record.get(attrib_name, default_val)

    def push_changes_to_db(self):
        """
        Send all updates to database.
        :return: None
        """
        if self._record_changes:
            self['ts_last_update'] = datetime.utcnow()
            if self.exists_in_db:
                self._db_connection.update_record(self.table_name, self.key, self._record_changes)
            else:
                # if new record get created, _record and _record_changes should contain same data
                self._db_connection.create_record(self.table_name, self.key, self._record)

            # reset record changes, because they were pushed to database, but leave record cache, it may be still needed
            # (but probably won't)
            self._record_changes = {}
