import logging


class Record:
    """
    Internal cache for database record and its updates to prevent loading whole record from database,
    but just record's attributes, which are meant to be updated
    """
    def __init__(self, db, etype, ekey):
        self._db_connection = db

        self._log = logging.getLogger("Record")

        self.etype = etype
        self.ekey = ekey

        # record structure
        self._record = {}
        # structure, which will hold all attributes, which were updated, all their updated values
        self._record_changes = {}

    def __del__(self):
        # TODO Maybe should push _record_changes to db before closing the connection
        pass
        # destructor, close connection to database - This should probably do database object itself
        # self._db_connection.close()

    def _load_from_db(self, key):
        # loads attribute from database into internal structure
        self._record[key] = self._db_connection.get_attrib(self.etype, self.ekey, key)

    def __getitem__(self, key):
        # overrides functionality, when "Record[key]" is called
        value = self._record.get(key)
        # if attribute is not loaded into internal structure yet, load it from database
        if value is None:
            self._load_from_db(key)

        return self._record.get(key)

    def __setitem__(self, key, value):
        # overrides functionality, when "Record[key] = value" is called
        self._record[key] = value
        # cache the changes
        self._record_changes[key] = value

    def exists(self):
        # checks, whether record exists in database
        return self._db_connection.exists(self.etype, self.ekey)

    def update(self, dict_update):
        # behaves like classic dict.update()
        self._record.update(dict_update)
        self._record_changes.update(dict_update)

    def get(self, key, default_val, load_from_db=True):
        """
        Behaves same as Dict's get, but can force loading record from database with 'load_from_db' flag
        """
        value = self._record.get(key)
        if value is None and load_from_db:
            self._load_from_db(key)

        return self._record.get(key, default_val)

    def push_changes_to_db(self):
        # send all updates to database
        self._db_connection.update(self.etype, self.ekey, self._record_changes)
        # reset record changes, because they were pushed to database, but leave record cache, it may be needed
        # (but probably won't)
        self._record_changes = {}
