from datetime import datetime
import sys
import inspect
from collections import deque, Iterable, OrderedDict, Counter
import logging

from src.common.utils import get_func_name

ENTITY_TYPES = ['ip', 'asn', 'bgppref', 'ipblock', 'org']


class TaskExecutor:
    """
    TaskExecutor manages updates of entity records, which are being read from task queue (via parent TaskDistributor)

    All operation function names must start with the string in OPERATION_FUNCTION_PREFIX. TaskExecutor can then load
    all operation functions into mapping dictionary and operation functions are easily extendable with new operation.
    So template for operation function is:

    def _perform_op_<name_of_operation>(self, rec, key, updreq):
        ...

    TODO this should be probably placed to corresponding function and the whole description should be in wiki instead
    Update request specification = list of n-tuples:
    - [(op, key, params...), ...]
      - ('set', key, value)        - set new value to given key (rec[key] = value)
      - ('append', key, value)     - append new value to array at key (rec[key].append(key))
      - ('add_to_set', key, value) - append new value to array at key if it isn't present in the array yet (if value not in rec[key]: rec[key].append(value))
      - ('extend_set', key, iterable) - append values from iterable to array at key if the value isn't present in the array yet (for value in iterable: if value not in rec[key]: rec[key].append(value))
      - ('rem_from_set', key, iterable) - remove all values at key which are specified in an array
      - ('add', key, value)        - add given numerical value to that stored at key (rec[key] += value)
      - ('sub', key, value)        - subtract given numerical value from that stored at key (rec[key] -= value)
      - ('setmax', key, value)     - set new value of the key to larger of the given value and the current value (rec[key] = max(value, rec[key]))
      - ('setmin', key, value)     - set new value of the key to smaller of the given value and the current value (rec[key] = min(value, rec[key]))
      - ('remove', key)            - remove given key (and all subkeys) from the record (parameter is ignored) (do nothing if the key doesn't exist)
      - ('next_step', key, key_base, min, step) - set value of 'key' to the smallest value of 'rec[key_base] + N*step' that is greater than 'min' (used by updater to set next update time); key_base MUST exist in the record!
      - ('array_update', key, query, actions) - apply given actions to specified array item under key, see below for details.
      - ('array_upsert', key, query, actions) - apply given actions to specified array item under key (insert new item if no one matches), see below for details.
      - ('array_remove', key, query) - remove array item satisfying given query (do nothing if no one matches).
      - ('event', !name)    - do nothing with record, only trigger functions hooked on the event name
    The tuple is passed to functions watching for updates of given keys / events
    with given name. Event names must begin with '!' (attribute keys mustn't).
    Update manager performs the requested update and calls functions hooked on
    the attribute/event.
    A hooked function receives a list of updates that triggered its call,
    i.e. a list of 2-tuples (attr_name, new_value) or (event_name, param)
    (if more than one update triggers the same function, it's called only once).

    Special actions "array_update"/"array_upsert":
      - ('array_update', key, query, actions)
    "key" must be path to an array of objects (dicts),
    the item whose values match those in "query" dict is selected
    all "actions" are performed with the selected object (keys inside those actions should be relative to the object root).
    If the array does not contain any matching item:
      - array_update: record is not changed
      - array_upsert: "query" is added as a new array item.
    If there are multiple matching events, only the first one is used.
    "actions" may contain actions of type "array_update"/"array_upsert" (recursion), it must not contain events.
    Rationale:
      Because of DB constraints, keys should always be fixed values. Therefore we often use
      arrays of subobjects where one or more attributes of the subobject act as a key.
      This action type allows to work with such structures.
    Examples:
      ('array_update', 'bl', {n: "blacklistname"} , [('set', 'v', 1), ('set', 't', req_time), ('append', 'h', req_time)])
      ('array_upsert', 'events', {date: "2017-07-17", cat: "ReconScanning"} , [('add', 'n', 1)])
    """

    _OPERATION_FUNCTION_PREFIX = "_perform_op_"

    def __init__(self, config, db, process_index, num_processes):
        # initialize task distribution
        super().__init__(config, process_index, num_processes)

        self.log = logging.getLogger("TaskExecutor")

        # TODO handler attribute mapping - check after implementation of register_handler()
        # Mapping of names of attributes to a list of functions that should be
        # called when the attribute is updated
        # (One such mapping for each entity type)
        self._attr2func = {etype: {} for etype in ENTITY_TYPES}
        # Set of attributes that may be updated by a function
        self._func2attr = {etype: {} for etype in ENTITY_TYPES}
        # Mapping of functions to set of attributes the function watches, i.e.
        # is called when the attribute is changed
        self._func_triggers = {etype: {} for etype in ENTITY_TYPES}

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
        rec[key] = updreq[2]
        return [(updreq[1], rec[key])]

    def _perform_op_append(self, rec, key, updreq):
        if key not in rec:
            rec[key] = [updreq[2]]
        else:
            rec[key].append(updreq[2])
        return[(updreq[1], rec[key])]

    def _perform_op_add_to_set(self, rec, key, updreq):
        value = updreq[2]
        if key not in rec:
            rec[key] = [value]
        elif value not in rec[key]:
            rec[key].append(value)
        else:
            return None
        return [(updreq[1], rec[key])]

    def _perform_op_extend_set(self, rec, key, updreq):
        value = updreq[2]
        if key not in rec:
            rec[key] = list(value)
        else:
            changed = False
            for val in value:
                if val not in rec[key]:
                    rec[key].append(val)
                    changed = True
            if not changed:
                return None
        return [(updreq[1], rec[key])]

    def _perform_op_rem_from_set(self, rec, key, updreq):
        if key in rec:
            rec[key] = list(set(rec[key]) - set(updreq[2]))
        return [(updreq[1], rec[key])]

    def _perform_op_add(self, rec, key, updreq):
        if key not in rec:
            rec[key] = updreq[2]
        else:
            rec[key] += updreq[2]
        return [(updreq[1], rec[key])]

    def _perform_op_sub(self, rec, key, updreq):
        if key not in rec:
            rec[key] = -updreq[2]
        else:
            rec[key] -= updreq[2]
        return [(updreq[1], rec[key])]

    def _perform_op_setmax(self, rec, key, updreq):
        if key not in rec:
            rec[key] = updreq[2]
        else:
            rec[key] = max(updreq[2], rec[key])
        return [(updreq[1], rec[key])]

    def _perform_op_setmin(self, rec, key, updreq):
        if key not in rec:
            rec[key] = updreq[2]
        else:
            rec[key] = min(updreq[2], rec[key])
        return [(updreq[1], rec[key])]

    def _perform_op_remove(self, rec, key, updreq):
        if key in rec:
            del rec[key]
            return [(updreq[1], None)]
        return None

    def _perform_op_next_step(self, rec, key, updreq):
        key_base = updreq[2]
        minimum = updreq[3]
        step = updreq[4]
        base = rec[key_base]
        rec[key] = base + ((minimum - base) // step + 1) * step
        return [(updreq[1], rec[key])]

    def _op_array_update_or_upsert(self, rec, key, updreq, op):
        query = updreq[2]
        actions = updreq[3]
        if key not in rec:
            if op == "array_upsert":
                rec[key] = []
            else:
                return None  # Array doesn't exist and insert not requested
        array = rec[key]
        # Find the matching item in the array
        for i, item in enumerate(array):
            if all(item[a] == v for a, v in query.items()):
                break
        else:
            if op == "array_upsert":
                i = len(array)
                item = query
                array.append(query)
            else:
                return None  # No matching element found and insert not requested
        # Now, "item" is the selected array item ("i" its index), apply all actions to it
        updates_performed = []
        for action in actions:
            upds = self._perform_update(item, action)  # recursion
            # List of all actions must be returned, convert relative keys to absolute
            for inner_key, new_val in upds:
                updates_performed.append((key + '[' + str(i) + '].' + inner_key, new_val))
        return updates_performed

    def _perform_op_array_update(self, rec, key, updreq):
        self._op_array_update_or_upsert(rec, key, updreq, "array_update")

    def _perform_op_array_upsert(self, rec, key, updreq):
        self._op_array_update_or_upsert(rec, key, updreq, "array_upsert")

    def _perform_op_array_remove(self, rec, key, updreq):
        query = updreq[2]
        if key not in rec:
            return None
        array = rec[key]
        # Find the matching item in the array
        for i, item in enumerate(array):
            if all(item[a] == v for a, v in query.items()):
                break
        else:
            return None
        # Remove it
        del array[i]
        return [(key + '[' + str(i) + ']', None)]

    def _perform_update(self, rec, updreq):
        """
        Update a record according to given update request.

        updreq - n-tuple (op, key, params...)

        Return array with specifications of performed updates - pairs (updated_key,
        new_value) or None.
        (None is returned when nothing was changed, e.g. because op=add_to_set and
        value was already present, or removal of non-existent item was requested)
        """
        op = updreq[0]
        key = updreq[1]

        rec = self._parse_record_from_key_hierarchy(rec, key)

        try:
            # call operation function, which handles operation
            # Return tuple (updated attribute, new value)
            return self._operations_mapping[op](self, rec, key, updreq)
        except KeyError:
            print("ERROR: perform_update: Unknown operation {}".format(op), file=sys.stderr)
            return None

    def is_update_request_weak(self, update_requests):
        # Check whether a new record should not be created in case every operation is 'weak' (starts with '*')
        weak_op = True
        for ndx, updreq in enumerate(update_requests):
            op = updreq[0]
            if op[0] != '*':
                weak_op = False
            else:
                # Remove starting symbol '*'
                update_requests[ndx] = [updreq[0][1:]] + updreq[
                                                         1:]  # first item without first char + all other items
        return weak_op

    def create_record_if_does_not_exist(self, etype, eid, update_requests):
        """
        Create new record, if it does not exist, do not create new record if operation is weak.

        Arguments:
        etype - entity type
        eid - entity ID
        update_requests - list of n-tuples as described above
        """
        new_rec_created = False
        rec = self.db.get(etype, eid)
        if rec is None:
            if self.is_update_request_weak(update_requests):
                update_requests.clear()
                self.log.debug(
                    "Received only weak operations for non-existent entity {} of type {}. Aborting record creation.".format(
                        etype, eid))
            else:
                now = datetime.utcnow()
                rec = {
                    '_id': eid,
                    'ts_added': now,
                    'ts_last_update': now,
                }
                new_rec_created = True
                # New record was created -> add "!NEW" event to update_request
                # self.log.debug("New record ({},{}) was created, injecting event '!NEW'".format(etype,eid))
                update_requests.insert(0, ('event', '!NEW'))
        return rec, new_rec_created

    # TODO cache results (clear cache when register_handler is called)
    def get_all_possible_changes(self, etype, attr):
        """
        Returns all attributes (as a set) that may be changed by a "chain reaction"
        of changes triggered by update of given attribute (or event).

        Warning: There must be no loops in the sequence of attributes and
        triggered functions.
        """
        may_change = set()  # Attributes that may be changed
        funcs_to_call = set(self._attr2func[etype].get(attr, ()))
        f2a = self._func2attr[etype]
        a2f = self._attr2func[etype]
        while funcs_to_call:
            func = funcs_to_call.pop()
            attrs_to_change = f2a[func]
            may_change.update(attrs_to_change)
            for attr in attrs_to_change:
                funcs_to_call |= set(a2f.get(attr, ()))
        return may_change

    def _delete_record_from_db(self, etype, ekey):
        pass

    def process_task(self, task):
        """
        Main processing function - update attributes or trigger an event.

        :param: task is 8-tuple, which consists of:
            etype: entity type (eg. 'ip')
            ekey: entity key ('192.0.2.42')
            attr_updates: TODO
            events: list of events to issue (just plain strings, as event parameters are not needed, may be added in the future if needed)
            create: true = create a new record if it doesn't exist yet; false = don't create a record if it
                    doesn't exist (like "weak" in NERD); not set = use global configuration flag "auto_create_record" of the entity type
            delete: delete the record
            src: name of source module, mostly for logging
            tags: tags for logging (number of tasks per time interval by tag)

        :return: True if a new record was created, False otherwise.
        """
        # Load record corresponding to the key from database.
        # If record doesn't exist, create new.
        # Also create associated auxiliary objects:
        #   call_queue - queue of functions that should be called to update the record.
        #     queue.Queue of tuples (function, list_of_update_spec), where list_of_update_spec is a list of
        #     updates (2-tuples (key, new_value) or (event, param) which triggered the function.
        #   may_change - set of attributes that may be changed by planned function calls

        etype, ekey, attr_updates, events, create, delete, src, tags = task

        if delete:
            self._delete_record_from_db(etype, ekey)
            # TODO what next after deletion?

        # Fetch the record from database or create a new one, new_rec_created is just boolean flag
        rec, new_rec_created = self.create_record_if_does_not_exist(etype, ekey, attr_updates)

        # TODO refactor the rest of the method, mainly split into smaller peaces and rework 'may_change' mechanism,
        # TODO because it is probably broken

        # Short-circuit if update_requests is empty (used to only create a record if it doesn't exist)
        if not attr_updates:
            return False

        requests_to_process = attr_updates

        # *** Now we have the record, process the requested updates ***

        # auxiliary objects
        call_queue = deque()  # planned calls of handler functions due to their hooking to attribute updates
        may_change = set()  # which attributes may change after performing all calls in call_queue

        loop_counter = 0  # counter used to stop when looping too long - probably some cycle in attribute dependencies

        deletion = False
        # *** call_queue loop ***
        while True:
            # *** If any update requests are pending, process them ***
            # (i.e. perform requested changes, add calls to hooked functions to
            # the call_queue and update the set of attributes that may change)
            if requests_to_process:
                # Process update requests (perform updates, put hooked functions to call_queue and update may_change set)
                # self.log.debug("UpdateManager: New update requests for ({},{}): {}".format(etype, eid, requests_to_process))
                for updreq in requests_to_process:
                    op = updreq[0]
                    attr = updreq[1]
                    assert (op != 'event' or attr[0] == '!'), "if op=event, attr must begin with '!'"

                    if op == 'event':
                        # self.log.debug("Initial update: Event ({}:{}).{} (param={})".format(etype,eid,attr,val))
                        updated = [(attr, None)]

                        # Check whether the event is !DELETE, clear queues and add calls to functions hooked to the !DELETE event
                        if attr == '!DELETE':
                            deletion = True
                            requests_to_process.clear()
                            call_queue.clear()
                            for func in self._attr2func[etype].get(attr, []):
                                call_queue.append((func, updated))
                            break
                    else:
                        # self.log.debug("Initial update: Attribute update: ({}:{}).{} [{}] {}".format(etype,eid,attr,op,val))
                        updated = self._perform_update(rec, updreq)
                        if not updated:
                            # self.log.debug("Attribute value wasn't changed.")
                            continue

                    # Add to the call_queue all functions directly hooked to the attribute/event
                    for func in self._attr2func[etype].get(attr, []):
                        # If the function is already in the queue...
                        for f, updates in call_queue:
                            if f == func:
                                # ... just add upd to list of updates that triggered it
                                # TODO FIXME: what if one attribute is updated several times? It should be in the list only once, with the latest value.
                                updates.extend(updated)
                                break
                        # Otherwise put the function to the queue
                        else:
                            call_queue.append((func, updated))

                    # Compute all attribute changes that may occur due to this event and add
                    # them to the set of attributes to change
                    # self.log.debug("get_all_possible_changes: {} -> {}".format(str(attr), repr(self.get_all_possible_changes(etype, attr))))
                    may_change |= self.get_all_possible_changes(etype, attr)
                    # self.log.debug("may_change: {}".format(may_change))

                # All requests were processed, clear the list
                requests_to_process.clear()

            if not call_queue:
                break  # No more work to do

            # *** Do all function calls planned in the call queue ***

            #             self.log.debug("call_queue loop iteration {}:\n  call_queue: {}\n  may_change: {}".format(
            #                 loop_counter,
            #                 list(map(lambda x: (get_func_name(x[0]), x[1]), call_queue)),
            #                 may_change)
            #             )
            # safety check against infinite looping
            loop_counter += 1
            if loop_counter > 20:
                self.log.warning(
                    "Too many iterations when updating ({}:{}), something went wrong! Update chain stopped.".format(
                        etype, ekey))
                break

            func, updates = call_queue.popleft()

            # If the function watches some attributes that may be updated later due
            # to expected subsequent events, postpone its call.
            if may_change & self._func_triggers[etype][func]:  # nonempty intersection of two sets
                # Put the function call back to the end of the queue
                # self.log.debug("call_queue: Postponing call of {}({})".format(get_func_name(func), updates))
                call_queue.append((func, updates))
                continue

            # Call the event handler function.
            # Set of requested updates of the record should be returned
            # self.log.debug("Calling: {}(({}, {}), rec, {})".format(get_func_name(func), etype, eid, updates))
            #            t_handler1 = time.time()
            try:
                reqs = func((etype, ekey), rec, updates)
            except Exception as e:
                self.log.exception("Unhandled exception during call of {}(({}, {}), rec, {}). Traceback follows:"
                                   .format(get_func_name(func), etype, ekey, updates))
                reqs = []
            #            t_handler2 = time.time()
            #            t_handlers[get_func_name(func)] = t_handler2 - t_handler1

            # Set requested updates to requests_to_process
            if reqs:
                requests_to_process.extend(reqs)

            # TODO FIXME - toto asi predpoklada, ze urcity atribut muze byt menen jen jednou handler funkci
            # (coz jsem mozna nekde zadal jako nutnou podminku; kazdopadne jestli to tak je, musi to byt nekde velmi jasne uvedeno)
            # Remove set of possible attribute changes of that function from
            # may_change (they were either already changed (or are in requests_to_process) or they won't be changed)
            # self.log.debug("Removing {} from may_change.".format(self._func2attr[etype][func]))
            may_change -= set(self._func2attr[etype][func])
            # self.log.debug("New may_change: {}".format(may_change))

        # self.log.debug("call_queue loop end")
        # FIXME: Temporarily disabled, removing from may_change doesn't work well, the whole algorithm must be reworked
        # assert(len(may_change) == 0)

        # Set ts_last_update
        rec['ts_last_update'] = datetime.utcnow()

        #        t3 = time.time()

        # Remove or update processed database record
        if deletion:
            self.db.delete(etype, ekey)
            self.log.debug("Entity '{}' of type '{}' was removed from the database.".format(ekey, etype))
        else:
            self.db.put(etype, ekey, rec)

        #        t4 = time.time()
        #        #if t4 - t1 > 1.0:
        #        #    self.log.info("Entity ({}:{}): load: {:.3f}s, process: {:.3f}s, store: {:.3f}s".format(etype, eid, t2-t1, t3-t2, t4-t3))
        #        #    self.log.info("  handlers:" + ", ".join("{}: {:.3f}s".format(fname, t) for fname, t in t_handlers))
        #
        #        self.t_handlers.update(t_handlers)

        return new_rec_created
