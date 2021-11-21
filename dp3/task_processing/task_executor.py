from datetime import datetime
import sys
import inspect
from collections import deque, Iterable, OrderedDict, Counter
import logging
from copy import deepcopy

from event_count_logger import EventCountLogger, DummyEventGroup

from .. import g
from ..common.utils import get_func_name
from ..database.record import Record


class TaskExecutor:
    """
    TaskExecutor manages updates of entity records, which are being read from task queue (via parent TaskDistributor)

    All operation function names must start with the string in OPERATION_FUNCTION_PREFIX. TaskExecutor can then load
    all operation functions into mapping dictionary and operation functions are easily extendable with new operation.
    So template for operation function is:

    def _perform_op_<name_of_operation>(self, rec, key, updreq):
        ...
        return [key, rec[key]]   # second value is rec[key], if the value was updated, else it's None

    TODO this should be probably placed to corresponding function and the whole description should be in wiki instead
    Update request specification = dictionary with keys 'attr' (attribute's name) and 'op' (name of required operation),
    other attributes of update request differs based on operation type. List of operations and their additional attribs
    (aa) follows:
      - 'set', aa: val                - set new value to given key (rec[attr] = val)
      - 'unset'                       - Unset attribute's value, which means set it to NULL/None (do nothing if the attrib doesn't exist)
      - 'add', aa: val                - add given numerical value to that stored at key (rec[attr] += val)
      - 'sub', aa: val                - subtract given numerical value from that stored at key (rec[attr] -= val)
      - 'setmax', aa: val             - set new value of the key to larger of the given value and the current value (rec[attr] = max(val, rec[attr]))
      - 'setmin', aa: val             - set new value of the key to smaller of the given value and the current value (rec[attr] = min(val, rec[attr]))
      - 'next_step', base_attr, min, step - set value of 'attr' to the smallest value of 'rec[base_attr] + N*step' that is greater than 'min' (used by updater to set next update time); base_attr MUST exist in the record!
      - 'array_append', aa: val       - append new value to array at key (rec[attr].append(val))
      - 'array_insert', aa: i, val    - insert value at (before) the i-th position of the array. (rec[attr].insert(i, val))
      - 'array_remove', aa: val       - remove value from the array (do nothing if val is not present)
      - 'set_add', aa: value          - append new value to array at key if it isn't present in the array yet (if value not in rec[key]: rec[key].append(value))
      - 'rem_from_set', aa: val       - remove all values at key which are specified in an array

    A hooked function receives a list of updates that triggered its call,
    i.e. a list of 2-tuples (attr_name, new_value) or (event_name, param)
    (if more than one update triggers the same function, it's called only once).

    If there are multiple matching events, only the first one is used.

    :param db: Instance of EntityDatabase
    :param attr_spec: Configuration of entity types and attributes (dict entity_name->entity_spec)
    """

    _OPERATION_FUNCTION_PREFIX = "_perform_op_"

    def __init__(self, db, attr_spec):
        # initialize task distribution

        self.log = logging.getLogger("TaskExecutor")

        # Get list of configured entity types
        self.entity_types = list(attr_spec.keys())
        self.log.debug(f"Configured entity types: {self.entity_types}")

        # Mapping of names of attributes to a list of functions that should be
        # called when the attribute is updated
        # (One such mapping for each entity type)
        self._attr2func = {etype: {} for etype in self.entity_types}
        # Set of attributes that may be updated by a function
        self._func2attr = {etype: {} for etype in self.entity_types}
        # Mapping of functions to set of attributes the function watches, i.e.
        # is called when the attribute is changed
        self._func_triggers = {etype: {} for etype in self.entity_types}
        # cache for may_change set calculation - is cleared when register_handler() is called
        self._may_change_cache = self._init_may_change_cache()

        self.attr_spec = attr_spec
        self.db = db
        # get all update operations functions into callable dictionary, where key is operation name and value is
        # callable function, which executes the operation, will look like:
        # {
        #   'set': _perform_op_set,
        #   'append' _perform_op_append,
        #   ...
        # }
        self._operations_mapping = {}
        for class_function in inspect.getmembers(TaskExecutor, predicate=inspect.isfunction):
            if class_function[0].startswith(TaskExecutor._OPERATION_FUNCTION_PREFIX):
                self._operations_mapping[class_function[0][len(TaskExecutor._OPERATION_FUNCTION_PREFIX):]] = class_function[1]

        # EventCountLogger - count number of events across multiple processes using shared counters in Redis
        ecl = EventCountLogger(g.config.get("event_logging.groups"), g.config.get("event_logging.redis"))
        self.elog = ecl.get_group("te") or DummyEventGroup()
        self.elog_by_src = ecl.get_group("tasks_by_src") or DummyEventGroup()
        self.elog_by_tag = ecl.get_group("tasks_by_tag") or DummyEventGroup()

    def _init_may_change_cache(self):
        """
        Initializes _may_change_cache with all supported Entity types
        :return: None
        """
        may_change_cache = {}
        for etype in self.entity_types:
            may_change_cache[etype] = {}
        return may_change_cache

    def register_handler(self, func, etype, triggers, changes):
        """
        Hook a function (or bound method) to specified attribute changes/events. Each function must be registered only
        once! Type check is already done in TaskDistributor.
        :param func: function or bound method (callback)
        :param etype: entity type (only changes of attributes of this etype trigger the func)
        :param triggers: set/list/tuple of attributes whose update trigger the call of the method (update of any one of the attributes will do)
        :param changes: set/list/tuple of attributes the method call may update (may be None)
        :return: None
        """
        # need to clear calculations in set, because may be wrong now
        self._may_change_cache = self._init_may_change_cache()
        # _func2attr[etype]: function -> list of attrs it may change
        # _func_triggers[etype]: function -> list of attrs that trigger it
        # _attr2func[etype]: attribute -> list of functions its change triggers
        # There are separate mappings for each entity type.
        self._func2attr[etype][func] = tuple(changes) if changes is not None else ()
        self._func_triggers[etype][func] = set(triggers)
        for attr in triggers:
            if attr in self._attr2func[etype]:
                self._attr2func[etype][attr].append(func)
            else:
                self._attr2func[etype][attr] = [func]

    def _parse_record_from_key_hierarchy(self, rec, key):
        # Process keys with hierarchy, i.e. containing dots (like "events.scan.count")
        # rec will be the inner-most subobject ("events.scan"), key the last attribute ("count")
        # If path doesn't exist in the hierarchy, it's created
        while '.' in key:
            first_key, key = key.split('.', 1)
            if first_key.isdecimal():  # index of array
                rec = rec[int(first_key)]
            else:  # key of object/dict
                if first_key not in rec:
                    rec[first_key] = {}
                rec = rec[first_key]
        return rec

    def _perform_op_set(self, rec, key, updreq):
        rec[key] = updreq['val']
        return [(key, rec[key])]

    def _perform_op_unset(self, rec, key, updreq):
        if key in rec:
            del rec[key]
            return [(updreq[1], None)]
        return None

    def _perform_op_add(self, rec, key, updreq):
        if key not in rec:
            rec[key] = updreq['val']
        else:
            rec[key] += updreq['val']
        return [(key, rec[key])]

    def _perform_op_sub(self, rec, key, updreq):
        if key not in rec:
            rec[key] = -updreq['val']
        else:
            rec[key] -= updreq['val']
        return [(key, rec[key])]

    def _perform_op_setmax(self, rec, key, updreq):
        if key not in rec:
            rec[key] = updreq['val']
        else:
            rec[key] = max(updreq['val'], rec[key])
        return [(key, rec[key])]

    def _perform_op_setmin(self, rec, key, updreq):
        if key not in rec:
            rec[key] = updreq['val']
        else:
            rec[key] = min(updreq['val'], rec[key])
        return [(key, rec[key])]

    def _perform_op_next_step(self, rec, key, updreq):
        key_base = updreq['base_attr']
        minimum = updreq['min']
        step = updreq['step']
        base = rec[key_base]
        rec[key] = base + ((minimum - base) // step + 1) * step
        return [(key, rec[key])]

    def _perform_op_array_append(self, rec, key, updreq):
        if key not in rec:
            rec[key] = [updreq['val']]
        else:
            rec[key].append(updreq['val'])
        return[(key, rec[key])]

    def _perform_op_array_insert(self, rec, key, updreq):
        if not isinstance(rec[key], list):
            return None
        rec[key] = rec[key].insert(updreq['i'], updreq['val'])
        return [(key, rec[key])]

    def _perform_op_array_remove(self, rec, key, updreq):
        if key not in rec:
            return None
        rec[key].remove(updreq['val'])
        return [(key, None)]

    def _perform_op_set_add(self, rec, key, updreq):
        value = updreq['val']
        if key not in rec:
            rec[key] = [value]
        elif value not in rec[key]:
            rec[key].append(value)
        else:
            return None
        return [(key, rec[key])]

    def _perform_op_set_remove(self, rec, key, updreq):
        if key in rec:
            rec[key] = list(set(rec[key]) - set(updreq['val']))
        return [(key, rec[key])]

    def _perform_update(self, rec: Record, updreq: dict):
        """
        Update a record according to given update request.

        :param rec: Instance of Record, which is used for communication with database
        :param updreq: 3-tuple in form of (operation, key, value)
        :return: array with specifications of performed updates - pairs (updated_key,
                new_value) or None.
            (None is returned when nothing was changed, e.g. because op=add_to_set and
            value was already present, or removal of non-existent item was requested)
        """
        op = updreq['op']
        key = updreq['attr']

        # TODO: will this really change the "rec" (Record) correctly, i.e. it seems it's "update" method is not called,
        #  so the changes won't get written to database
        #  (the above holds if there is a hierarchical key, since _parse_record_from_key_hierarchy return normal object, not Record, in such case)
        rec = self._parse_record_from_key_hierarchy(rec, key)

        try:
            # call operation function, which handles operation
            # Return tuple (updated attribute, new value)
            return self._operations_mapping[op](self, rec, key, updreq)
        except KeyError:
            print("ERROR: perform_update: Unknown operation {}".format(op), file=sys.stderr)
            return None

    def _create_record_if_does_not_exist(self, etype: str, ekey: str, attr_updates: list, events: list, create: bool) -> (Record, bool):
        """
        Create new record, if it does not exist, do not create new record if operation is weak.

        :param etype: - entity type
        :param ekey: - entity key
        :param attr_updates: list of n-tuples as described above
        :param events: list of task events
        :param create: boolean flag, which determines, whether new record should be created or not if not in database yet
        """
        new_rec_created = False
        rec = Record(self.db, etype, ekey)
        if not rec.exists_in_db:
            if not create:
                attr_updates.clear()
                self.log.debug(
                    "Received task with 'create' = false for non-existent entity {} of type {}. Aborting record "
                    "creation.".format(etype, ekey))
            else:
                now = datetime.utcnow()
                rec.update({
                    'eid': ekey,
                    'ts_added': now,
                    'ts_last_update': now,
                    '_lru': now,
                })
                new_rec_created = True
                # New record was created -> add "!NEW" event
                # self.log.debug("New record ({},{}) was created, injecting event '!NEW'".format(etype,eid))
                events.insert(0, "!NEW")
        return rec, new_rec_created

    def get_all_possible_changes(self, etype: str, trigger_attr_name: str) -> set:
        """
        Returns all attributes (as a set) that may be changed by a "chain reaction"
        of changes triggered by update of given attribute (or event).

        Warning: There must be no loops in the sequence of attributes and
        triggered functions.
        :param etype: type of entity (e.g. 'ip')
        :param trigger_attr_name: name of attribute or event, which update triggered the call
        :return set of attribute names
        """
        # first try to find result in cache, if it does not exist, calculate it
        try:
            return self._may_change_cache[etype][trigger_attr_name]
        except KeyError:
            pass
        # Attributes that may be changed
        may_change = set()
        # get all functions, which are hooked on attribute change
        funcs_to_call = set(self._attr2func[etype].get(trigger_attr_name, ()))
        f2a = self._func2attr[etype]
        a2f = self._attr2func[etype]
        # iteratively find to every function attributes, which may be changed by the function and to all those attribs
        # find their hooked functions, which are added to funcs_to_call (so their attributes will be checked too)
        while funcs_to_call:
            func = funcs_to_call.pop()
            attrs_to_change = f2a[func]
            may_change.update(attrs_to_change)
            for attr_name in attrs_to_change:
                funcs_to_call |= set(a2f.get(attr_name, ()))
        self._may_change_cache[etype][trigger_attr_name] = may_change
        return may_change

    def _delete_record_from_db(self, etype: str, ekey: str) -> None:
        self.db.delete(etype, ekey)

    def _update_call_queue(self, call_queue: deque, etype: str, attr_name: str, updated: list) -> None:
        """
        Add all functions, which are hooked to the attribute/event, to the call queue
        :param call_queue: call_queue for storing function callbacks
        :param updated: tuple with attribute's name and it's new value
        :return: None
        """
        for func in self._attr2func[etype].get(attr_name, []):
            for f, updates in call_queue:
                if f == func:
                    # if the attribute was already updated earlier, then remove it before extend, it should
                    # be there only once
                    attrib_names = [update[0] for update in updates]
                    if updated[0] in attrib_names:
                        del updates[attrib_names.index(updated[0])]
                    # ... just add upd to list of updates that triggered it
                    updates.extend(updated)
                    break
            else:
                # Otherwise put the function to the queue
                call_queue.append((func, updated))

    def process_task(self, task: tuple):
        """
        Main processing function - update attributes or trigger an event.

        :param: task is 8-tuple, which consists of:
            etype: entity type (eg. 'ip')
            ekey: entity key ('192.0.2.42')
            attr_updates: dictionary specifying attrbute, which will be updated, operation and its arguments
                        e.g. {"attr": "some_attribute", "op": "set", "val": "example"}
            events: list of events to issue (just plain strings, as event parameters are not needed, may be added in the
                    future if needed)
            data_points: list of attribute data points, which will be saved in the database
            create: true = create a new record if it doesn't exist yet; false = don't create a record if it doesn't
                    exist (like "weak" in NERD); not set = use global configuration flag "auto_create_record" of the
                    entity type
            delete: delete the record
            src: name of source module, mostly for logging
            tags: tags for logging (number of tasks per time interval by tag)

        :return: True if a new record was created, False otherwise.
        """
        etype, ekey, attr_updates, events, data_points, create, delete, src, tags = task

        self.log.debug(f"Received new task {etype}/{ekey}, starting processing!")

        # Check existence of etype
        if etype not in self.attr_spec:
            self.log.error(f"Task {etype}/{ekey}: Unknown entity type!")
            return False

        # whole record should be deleted from database
        if delete:
            self._delete_record_from_db(etype, ekey)
            self.elog.log('record_removed')
            self.log.debug(f"Task {etype}/{ekey}: Entity record removed from database.")
            return False

        if create is None:
            create = self.attr_spec[etype]['entity'].auto_create_record

        # Fetch the record from database or create a new one, new_rec_created is just boolean flag
        rec, new_rec_created = self._create_record_if_does_not_exist(etype, ekey, attr_updates, events, create)
        if new_rec_created:
            self.elog.log('record_created')
            self.log.debug(f"Task {etype}/{ekey}: New record created")

        # Short-circuit if attr_updates, events or data_points is empty (used to only create a record if it doesn't exist)
        if not attr_updates and not events and not data_points:
            self.log.debug(f"Task {etype}/{ekey}: Nothing to do, processing finished")
            return False

        requests_to_process = attr_updates

        # *** Now we have the record, process the requested updates ***

        # auxiliary objects
        call_queue = deque()  # planned calls of handler functions due to their hooking to attribute updates
        may_change = set()  # which attributes may change after performing all calls in call_queue

        loop_counter = 0  # counter used to stop when looping too long - probably some cycle in attribute dependencies

        # *** call_queue loop ***
        while True:
            # *** If any events or update requests are pending, process them ***
            # (i.e. events, perform requested changes, add calls to hooked functions to the call_queue and update
            # the set of attributes that may change)
            if requests_to_process or events or data_points:
                # process all data points
                for data_point in data_points:
                    # check of mandatory attributes
                    try:
                        _ = data_point['attr']
                        _ = data_point['t1']
                        _ = data_point['t2']
                    except KeyError as e:
                        self.log.error(f"Task {etype}/{ekey}: Data point has wrong structure! Missing key '{str(e)}', source: {data_point.get('src', '')}.")
                        continue
                    # prepare data to store, remove values which are not directly saved to database
                    data_to_save = deepcopy(data_point)
                    attr_name = data_to_save.pop('attr')
                    for key in ("type", "id"):
                        try:
                            data_to_save.pop(key)
                        except KeyError:
                            pass
                    # 'ekey' is saved as 'eid'
                    data_to_save['eid'] = ekey

                    # Store data-point to history table
                    # (current value is not automatically updated by DB wrapper, since we must update it here in
                    # the "rec" object and store to DB after all subsequent updates are done)
                    ok = self.db.create_datapoint(etype, attr_name, data_to_save)
                    if ok:
                        self.log.debug(f"Task {etype}/{ekey}: Data-point of '{attr_name}' stored: {data_to_save}")
                        # on error, a message is already logged by the create_datapoint() method

                    # TODO time aggregation
                    # ...

                    # Get current value and set it to the cached record ("rec", instance of Record)
                    current_value = self.db.get_current_value(etype, ekey, attr_name)
                    rec.update({attr_name: current_value})
                    self.log.debug(f"Task {etype}/{ekey}: Current value of '{attr_name}' updated to '{current_value}'")

                # add all functions, which are hooked to events, to call queue
                for event in events:
                    if isinstance(event, str):
                        event_name = event
                        updated = [(event_name, None)]
                    else:
                        # in case of future event format extension, event should be dict with event name under 'name'
                        try:
                            event_name = event['name']
                            updated = [(event_name, None)]
                        except (KeyError, TypeError):
                            self.log.warning("Event {event} has wrong structure!".format(event=event))
                            continue
                    self._update_call_queue(call_queue, etype, event_name, updated)

                    # Compute all attribute changes that may occur due to this event and add them to the set of
                    # attributes to change
                    may_change |= self.get_all_possible_changes(etype, event_name)

                # perform all update requests
                for update_request in requests_to_process:
                    attrib_name = update_request['attr']
                    updated = self._perform_update(rec, update_request)
                    self.log.debug(f"Task {etype}/{ekey}: Attribute value updated: {update_request} (value changed: {updated})")
                    if not updated:
                        continue

                    # Add all functions, which are hooked to the attribute to the call queue
                    self._update_call_queue(call_queue, etype, attrib_name, updated)

                    # Compute all attribute changes that may occur due to this update and add them to the set of
                    # attributes to change
                    may_change |= self.get_all_possible_changes(etype, attrib_name)

                # All requests were processed, clear the list
                requests_to_process.clear()
                events.clear()

            if not call_queue:
                break  # No more work to do

            # safety check against infinite looping
            loop_counter += 1
            if loop_counter > 20:
                self.log.warning(
                    "Too many iterations when updating ({}/{}), something went wrong! Update chain stopped.".format(
                        etype, ekey))
                break

            handler_function, updates = call_queue.popleft()

            # if the function watches some attributes that may be updated later due to expected subsequent events,
            # postpone its call
            if may_change & self._func_triggers[etype][handler_function]:  # nonempty intersection of two sets
                # put the function call back to the end of the queue
                # self.log.debug("call_queue: Postponing call of {}({})".format(get_func_name(func), updates))
                call_queue.append((handler_function, updates))
                continue

            # call the event handler function of some secondary module
            # set of requested updates of the record should be returned
            self.log.debug(f"Task {etype}/{ekey}: Calling handler function '{get_func_name(handler_function)}' ...")
            try:
                requested_updates = handler_function(etype, ekey, rec, updates)
            except Exception as e:
                self.log.exception("Unhandled exception during call of {}(({}, {}), rec, {}). Traceback follows:"
                                   .format(get_func_name(handler_function), etype, ekey, updates))
                requested_updates = []
                self.elog.log('module_error')
            self.log.debug(f"Task {etype}/{ekey}: New attribute update requests: '{requested_updates}'")

            # set requested updates to requests_to_process
            if requested_updates:
                requests_to_process.extend(requested_updates)

            # FIXME - toto asi predpoklada, ze urcity atribut muze byt menen jen jednou handler funkci
            # (coz jsem mozna nekde zadal jako nutnou podminku; kazdopadne jestli to tak je, musi to byt nekde velmi jasne uvedeno)
            # TODO think of the way, that attribute could be changed by multiple handler functions
            # Remove set of possible attribute changes of that function from
            # may_change (they were either already changed (or are in requests_to_process) or they won't be changed)
            may_change -= set(self._func2attr[etype][handler_function])
            # self.log.debug("New may_change: {}".format(may_change))

        # self.log.debug("call_queue loop end")
        assert(len(may_change) == 0)

        # Update processed database record
        rec.push_changes_to_db()

        # Log the processed task
        self.elog.log('task_processed')
        self.elog_by_src.log(src) # empty src is ok, empty string is a valid event id
        for tag in tags:
            self.elog_by_src.log(tag)

        self.log.debug(f"Task {etype}/{ekey}: All changes written to DB, processing finished.")

        return new_rec_created
