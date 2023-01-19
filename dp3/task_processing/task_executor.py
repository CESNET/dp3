import inspect
import logging
from collections import deque
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Union, Iterable

from event_count_logger import EventCountLogger, DummyEventGroup

from dp3.common.attrspec import AttrSpec
from dp3.common.entityspec import EntitySpec
from dp3.database.database import EntityDatabase
from dp3.history_management.history_manager import HistoryManager
from .. import g
from ..common.utils import get_func_name, parse_rfc_time
from ..database.record import Record
from ..history_management.history_manager import extrapolate_confidence


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

    def __init__(self, db: EntityDatabase,
                 attr_spec: dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpec]]]],
                 history_manager: HistoryManager) -> None:
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

        self.hm = history_manager
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
        # Print warning if some event group is not configured
        not_configured_groups = []
        if isinstance(self.elog, DummyEventGroup):
            not_configured_groups.append("te")
        if isinstance(self.elog_by_src, DummyEventGroup):
            not_configured_groups.append("tasks_by_src")
        if isinstance(self.elog_by_tag, DummyEventGroup):
            not_configured_groups.append("tasks_by_tag")
        if not_configured_groups:
            self.log.warning(f"EventCountLogger: No configuration for event group(s) '{','.join(not_configured_groups)}' found, such events will not be logged (check event_logging.yml)")

    def _init_may_change_cache(self) -> dict[str, dict[Any, Any]]:
        """
        Initializes _may_change_cache with all supported Entity types
        :return: None
        """
        may_change_cache = {}
        for etype in self.entity_types:
            may_change_cache[etype] = {}
        return may_change_cache

    def register_handler(self, func: Callable, etype: str, triggers: Iterable[str], changes: Iterable[str]) -> None:
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

    # def _parse_record_from_key_hierarchy(self, rec: Record, key: str):
    #     # Process keys with hierarchy, i.e. containing dots (like "events.scan.count")
    #     # Returned rec will be the inner-most subobject ("events.scan"), key the last attribute ("count")
    #     # If path doesn't exist in the hierarchy, it's created
    #     while '.' in key:
    #         first_key, key = key.split('.', 1)
    #         if first_key.isdecimal():  # index of array
    #             rec = rec[int(first_key)]
    #         else:  # key of object/dict
    #             if first_key not in rec:
    #                 rec[first_key] = {}
    #             rec = rec[first_key]
    #     return rec

    def _perform_op_set(self, rec: Record, key: str, updreq: dict):
        rec[key] = updreq['val']
        return [(key, rec[key])]

    def _perform_op_set_multivalue(self, rec: Record, key: str, updreq: dict):
        if key not in rec:
            rec[key] = [updreq['val']]
            if 'c' in updreq:
                rec[f"{key}:c"] = [updreq['c']]
            if 'exp' in updreq:
                rec[f"{key}:exp"] = [updreq['exp']]
        elif updreq['val'] in rec[key]:
            idx = rec[key].index(updreq['val'])
            if 'c' in updreq:
                rec[f"{key}:c"][idx] = updreq['c']
                # this is not no-op as it may seem, we need to perform item assignment on "rec" in order to trigger Record's custom __setitem__ method
                rec[f"{key}:c"] = rec[f"{key}:c"]
            if 'exp' in updreq:
                rec[f"{key}:exp"][idx] = updreq['exp']
                rec[f"{key}:exp"] = rec[f"{key}:exp"]
        else:
            rec[key] = rec[key] + [updreq['val']]
            if 'c' in updreq:
                # append is not used here, as we need to *assign* a new value to rec[key] in order for Record to cache what has changed (it has custom __setitem__ method)
                rec[f"{key}:c"] = rec[f"{key}:c"] + [updreq['c']]
            if 'exp' in updreq:
                rec[f"{key}:exp"] = rec[f"{key}:exp"] + [updreq['exp']]
        return [(key, rec[key])]

    def _perform_op_unset(self, rec: Record, key: str, updreq: dict):
        if key in rec:
            del rec[key]
            return [(updreq['attr'], None)]
        return None

    def _perform_op_unset_multivalue(self, rec: Record, key: str, updreq: dict):
        if key not in rec or updreq['val'] not in rec[key]:
            return None
        else:
            idx = rec[key].index(updreq['val'])
            del rec[key][idx]
            # this is not no-op as it may seem, we need to perform item assignment on "rec" in order to trigger Record's custom __setitem__ method
            rec[key] = rec[key]
            if f"{key}:c" in rec:
                del rec[f"{key}:c"][idx]
                rec[f"{key}:c"] = rec[f"{key}:c"]
            if f"{key}:exp" in rec:
                del rec[f"{key}:exp"][idx]
                rec[f"{key}:exp"] = rec[f"{key}:exp"]
        return [(key, rec[key])]

    def _perform_op_add(self, rec: Record, key: str, updreq: dict):
        if key not in rec:
            rec[key] = updreq['val']
        else:
            rec[key] += updreq['val']
        return [(key, rec[key])]

    def _perform_op_sub(self, rec: Record, key: str, updreq: dict):
        if key not in rec:
            rec[key] = -updreq['val']
        else:
            rec[key] -= updreq['val']
        return [(key, rec[key])]

    def _perform_op_setmax(self, rec: Record, key: str, updreq: dict):
        if key not in rec:
            rec[key] = updreq['val']
        else:
            rec[key] = max(updreq['val'], rec[key])
        return [(key, rec[key])]

    def _perform_op_setmin(self, rec: Record, key: str, updreq: dict):
        if key not in rec:
            rec[key] = updreq['val']
        else:
            rec[key] = min(updreq['val'], rec[key])
        return [(key, rec[key])]

    def _perform_op_next_step(self, rec: Record, key: str, updreq: dict):
        key_base = updreq['base_attr']
        minimum = updreq['min']
        step = updreq['step']
        base = rec[key_base]
        rec[key] = base + ((minimum - base) // step + 1) * step
        return [(key, rec[key])]

    def _perform_op_array_append(self, rec: Record, key: str, updreq: dict):
        if key not in rec:
            rec[key] = [updreq['val']]
        else:
            rec[key] = rec[key] + [updreq['val']]
        return[(key, rec[key])]

    def _perform_op_array_insert(self, rec: Record, key: str, updreq: dict):
        try:
            rec[key].insert(updreq['i'], updreq['val'])
            rec[key] = rec[key] # update record changes
        except Exception:
            return None
        return [(key, rec[key])]

    def _perform_op_array_remove(self, rec: Record, key: str, updreq: dict):
        try:
            rec[key].remove(updreq['val'])
            rec[key] = rec[key]  # update record changes
        except ValueError: # item is not in the array - do nothing
            return None
        return [(key, rec[key])]

    def _perform_op_set_add(self, rec: Record, key: str, updreq: dict):
        value = updreq['val']
        if key not in rec:
            rec[key] = [value]
        elif value not in rec[key]:
            rec[key].append(value)
            rec[key] = rec[key]  # update record changes
        else:
            pass
        return [(key, rec[key])]

    def _perform_op_set_remove(self, rec: Record, key: str, updreq: dict):
        if key in rec:
            rec[key] = list(set(rec[key]) - set(updreq['val']))
        return [(key, rec[key])]

    def _perform_update(self, rec: Record, updreq: dict, attrib_conf):
        """
        Update a record according to given update request.

        :param rec: Instance of Record, which is used for communication with database
        :param updreq: Dictionary containing operation, attribute id and value, optionally also confidence and/or expiration date
        :return: array with specifications of performed updates - pairs (updated_key,
                new_value) or None.
            (None is returned when nothing was changed, e.g. because op=add_to_set and
            value was already present, or removal of non-existent item was requested)
        """
        op = updreq['op']
        key = updreq['attr']

        # TODO: It seems this won't work correctly if there is a hierarchical key, since it returns normal object/dict,
        #  not Record, so any changes made over 'rec' are not cached in the Record object and therefore won't get
        #  written to database.
        #  Moreover, "key" is not updated, but it should be
        # Since we currently don't use hierarchical attribute names, this is disabled
        #rec = self._parse_record_from_key_hierarchy(rec, key)

        # check confidence and/or expiration date (if required by configuration) and set default values if needed
        if attrib_conf.confidence and 'c' not in updreq:
            updreq['c'] = 1
        if attrib_conf.type == "observations" and 'exp' not in updreq:
            updreq['exp'] = datetime.now()

        # multi value attributes have special operations that handle confidence/expiration on their own
        if attrib_conf.multi_value:
            op += "_multivalue"
        else:
            if attrib_conf.confidence:
                rec[f"{key}:c"] = updreq['c']
            if attrib_conf.type == "observations":
                rec[f"{key}:exp"] = updreq['exp']

        try:
            # call operation function, which handles operation
            # Return tuple (updated attribute, new value)
            return self._operations_mapping[op](self, rec, key, updreq)
        except KeyError as e:
            self.log.exception(f"perform_update: Unknown operation {op}, in call of {key} with {updreq}")
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
                    '_ttl': {}
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
        self.db.delete_record(etype, ekey)
    
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

        :param: task is 10-tuple, which consists of:
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
            ttl_token: time to live token

        :return: True if a new record was created, False otherwise.
        """
        etype, ekey, attr_updates, events, data_points, create, delete, src, tags, ttl_token = task

        self.log.debug(f"Received new task {etype}/{ekey}, starting processing!")

        # Check existence of etype
        if etype not in self.attr_spec:
            self.log.error(f"Task {etype}/{ekey}: Unknown entity type!")
            self.elog.log('task_processing_error')
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
 
        if ttl_token is not None and ttl_token != "":
            ttl = rec.get("_ttl", default_val={})
            ttl[ttl_token] = datetime.utcnow().isoformat('T')
            rec.update({
                "_ttl": ttl 
            })

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
                        self.elog.log('task_processing_error')
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
                    try:
                        self.hm.process_datapoint(etype, attr_name, data_to_save)
                        self.log.debug(f"Task {etype}/{ekey}: Data-point of '{attr_name}' stored: {data_to_save}")
                    except Exception as e:
                        # traceback.print_exc()
                        self.log.error(f"Task {etype}/{ekey}: Data-point of '{attr_name}' could not be stored: {e}")
                        self.elog.log('task_processing_error')
                        continue

                    # Update current value by adding corresponding update request to the current task
                    attr_conf = self.attr_spec[etype]['attribs'][attr_name]
                    if attr_conf.type == "observations":
                        valid_since = parse_rfc_time(data_point['t1']) - attr_conf.history_params['pre_validity']
                        valid_until = parse_rfc_time(data_point['t2']) + attr_conf.history_params['post_validity']
                        curr_expiration = rec[f"{attr_name}:exp"]
                        if valid_since < datetime.utcnow() < valid_until and (attr_conf.multi_value or curr_expiration is None or valid_until > curr_expiration):
                            update = {"attr": attr_name, "val": data_point['v'], "op": "set", "exp": valid_until}
                            if attr_conf.confidence:
                                update['c'] = extrapolate_confidence(datapoint={
                                    't1': parse_rfc_time(data_point['t1']), 't2': parse_rfc_time(data_point['t2']),
                                    'c': data_point['c']
                                }, timestamp=datetime.utcnow(), history_params=attr_conf.history_params)
                            requests_to_process.append(update)

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
                            self.log.error("Event {event} has wrong structure!".format(event=event))
                            self.elog.log('task_processing_error')
                            continue
                    self._update_call_queue(call_queue, etype, event_name, updated)

                    # Compute all attribute changes that may occur due to this event and add them to the set of
                    # attributes to change
                    may_change |= self.get_all_possible_changes(etype, event_name)

                # perform all update requests
                for update_request in requests_to_process:
                    attrib_name = update_request['attr']
                    updated = self._perform_update(rec, update_request, self.attr_spec[etype]['attribs'][attrib_name])
                    self.log.debug(f"Task {etype}/{ekey}: Attribute value updated: {update_request} (value changed: {updated})")
                    if not updated:
                        continue

                    # Add all functions, which are hooked to the attribute to the call queue
                    self._update_call_queue(call_queue, etype, attrib_name, updated)

                    # Compute all attribute changes that may occur due to this update and add them to the set of
                    # attributes to change
                    may_change |= self.get_all_possible_changes(etype, attrib_name)

                # All requests/events/datapoints were processed, clear the lists
                data_points.clear()
                events.clear()
                requests_to_process.clear()

            if not call_queue:
                break  # No more work to do

            # safety check against infinite looping
            loop_counter += 1
            if loop_counter > 20:
                self.log.warning(
                    "Too many iterations when updating ({}/{}), something went wrong! Update chain stopped.".format(
                        etype, ekey))
                self.elog.log('task_processing_error')
                may_change = {} # reset may_change to avoid triggering AssertionError below
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
        try:
            rec.push_changes_to_db()
            self.log.debug(f"Task {etype}/{ekey}: All changes written to DB, processing finished.")
        except Exception as e:
            self.log.error(f"Task {etype}/{ekey}: Something went wrong when pushing changes to DB: {e}")

        # Log the processed task
        self.elog.log('task_processed')
        self.elog_by_src.log(src) # empty src is ok, empty string is a valid event id
        for tag in tags:
            self.elog_by_src.log(tag)

        return new_rec_created
