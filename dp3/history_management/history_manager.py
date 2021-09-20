from datetime import datetime, timedelta
import time
import threading
import hashlib
import logging

from dp3.common.utils import parse_rfc_time
from dp3.database.record import Record
from copy import deepcopy

# Hash function used to distribute tasks to worker processes. Takes string, returns int.
# (last 4 bytes of MD5)
# Note: this is slightly different function than that in task_processing, but it doesn't matter, the processing here
# is independent
HASH = lambda x: int(hashlib.md5(x.encode('utf8')).hexdigest()[-4:], 16)

TAG_PLAIN = 0
TAG_AGGREGATED = 1
TAG_REDUNDANT = 2


class HistoryManager:
    def __init__(self, db, attr_spec, worker_index, num_workers):
        self.log = logging.getLogger("HistoryManager")
        self.db = db
        self.attr_spec = attr_spec
        self.worker_index = worker_index
        self.num_workers = num_workers

    def start(self):
        if self.worker_index == 0:
            self.log.info("Starting HistoryManager")
            self.running = True
            self.hm_thread = threading.Thread(target=self.history_management_thread, daemon=True)
            self.hm_thread.start()

    def stop(self):
        if self.worker_index == 0:
            self.running = False
            self.log.info("HistoryManager stopped")

    def process_datapoint(self, etype, attr_id, data):
        redundant_ids = []
        redundant_data = []
        delete_ids = []
        history_params = self.attr_spec[etype]['attribs'][attr_id].history_params
        multi_value = self.attr_spec[etype]['attribs'][attr_id].multi_value
        aggregation_interval = history_params["aggregation_interval"]
        t1 = parse_rfc_time(data['t1'])
        t2 = parse_rfc_time(data['t2'])
        data['tag'] = TAG_PLAIN

        # Check for collisions
        datapoints = self.db.get_datapoints_range(etype, attr_id, data['eid'], data['t1'], data['t2'])
        merge_list = []
        for d in datapoints:
            m = mergeable(data, d, history_params)
            merge_list.append(m)
            if not m and not d['tag'] == TAG_AGGREGATED and multi_value is False:
                raise ValueError("Incoming data point is overlapping with other, non mergeable data point(s)")

        # Merge with directly overlapping datapoints
        agg = deepcopy(data)
        for d in datapoints:
            is_mergeable = merge_list.pop(0)
            if d['tag'] == TAG_REDUNDANT:
                continue
            if is_mergeable:
                merge(agg, d, history_params)
                if d['tag'] == TAG_AGGREGATED:
                    delete_ids.append(d['id'])
                else:
                    d['tag'] = TAG_REDUNDANT
                    redundant_ids.append(d['id'])
                    redundant_data.append(d)
            elif multi_value is True:
                continue
            else:
                self.split_datapoint(etype, attr_id, d, t1)

        # Merge with non-overlapping datapoints
        pre = self.db.get_datapoints_range(etype, attr_id, data['eid'], str(t1 - aggregation_interval), data['t1'], closed_interval=False, sort=1)
        post = self.db.get_datapoints_range(etype, attr_id, data['eid'], data['t2'], str(t2 + aggregation_interval), closed_interval=False, sort=0)
        for d in pre + post:
            # TODO custom select function? get_datapoints_range() returns all datapoints overlapping with specified interval, we need them to be completely inside the interval here
            if d['t1'] <= t2 and t1 <= d['t2']:
                continue
            if d['tag'] == TAG_REDUNDANT:
                continue
            if mergeable(agg, d, history_params):
                merge(agg, d, history_params)
                if d['tag'] == TAG_AGGREGATED:
                    delete_ids.append(d['id'])
                else:
                    d['tag'] = TAG_REDUNDANT
                    redundant_ids.append(d['id'])
                    redundant_data.append(d)
            elif multi_value is True:
                continue
            else:
                break

        # Write changes to db
        if agg['t1'] != data['t1'] or agg['t2'] != data['t2']:
            agg['tag'] = TAG_AGGREGATED
            data['tag'] = TAG_REDUNDANT
            self.db.create_datapoint(etype, attr_id, agg)
        self.db.create_datapoint(etype, attr_id, data)
        if redundant_ids.__len__() > 0:
            self.db.rewrite_data_points(etype, attr_id, redundant_ids, redundant_data)
        if delete_ids.__len__() > 0:
            self.db.delete_multiple_records(f"{etype}__{attr_id}", delete_ids)

    def split_datapoint(self, etype, attr_id, data, timestamp):
        history_params = self.attr_spec[etype]['attribs'][attr_id].history_params
        redundant = self.db.get_datapoints_range(etype, attr_id, data['eid'], data['t1'], data['t2'], sort=0, tag=TAG_REDUNDANT)
        assert redundant.__len__() > 0, "split_datapoint(): unable to split, not enough data"
        assert redundant[0]['t1'] < timestamp, "split_datapoint(): unable to split, not enough data"
        agg = deepcopy(redundant[0])
        agg.pop('id')
        agg['t1'] = data['t1']  # in case some old data points got deleted
        flag = True
        for r in redundant[1:]:
            if flag and r['t1'] > timestamp:
                flag = False
                tmp1 = agg
                tmp2 = r
                agg = deepcopy(r)
                agg.pop('id')
                continue
            merge(agg, r, history_params)

        self.db.delete_record(f"{etype}__{attr_id}", data['id'])

        if tmp1['t2'] == redundant[0]['t2']:
            tmp1['tag'] = TAG_PLAIN
            self.db.delete_record(f"{etype}__{attr_id}", redundant[0]['id'])
        else:
            tmp1['tag'] = TAG_AGGREGATED
        self.db.create_datapoint(etype, attr_id, tmp1)

        if agg['t1'] == tmp2['t1']:
            agg['tag'] = TAG_PLAIN
            self.db.delete_record(f"{etype}__{attr_id}", tmp2['id'])
        else:
            agg['tag'] = TAG_AGGREGATED
        self.db.create_datapoint(etype, attr_id, agg)

    def process_datapoints_range(self, etype, eid, attr_id, t1, t2):
        delete_ids = []
        redundant_ids = []
        redundant_data = []
        history_params = self.attr_spec[etype]['attribs'][attr_id].history_params

        # TODO select non redundant (in a better way)
        d1 = self.db.get_datapoints_range(etype, attr_id, eid, t1, t2, sort=0, tag=TAG_PLAIN)
        d2 = self.db.get_datapoints_range(etype, attr_id, eid, t1, t2, sort=0, tag=TAG_AGGREGATED)
        datapoints = d1 + d2
        if not datapoints.__len__() > 0:
            return

        curr = deepcopy(datapoints[0])
        for d in datapoints[1:]:
            if mergeable(curr, d, history_params):
                merge(curr, d, history_params)
                if d['tag'] == TAG_AGGREGATED:
                    delete_ids.append(d['id'])
                else:
                    d['tag'] = TAG_REDUNDANT
                    redundant_ids.append(d['id'])
                    redundant_data.append(d)
            else:
                curr = d
        if redundant_ids.__len__() > 0:
            self.db.rewrite_data_points(etype, attr_id, redundant_ids, redundant_data)
        if delete_ids.__len__() > 0:
            self.db.delete_multiple_records(f"{etype}__{attr_id}", delete_ids)

    def history_management_thread(self):
        # TODO docstring explaining what the function does
        # TODO: use apscheduler
        tick_rate = timedelta(minutes=30)  # TODO add to global config
        next_call = datetime.now()
        while self.running:
            t_start = datetime.now()
            self.log.debug("Starting periodic history management function...")

            # Delete old records from history tables
            for etype in self.attr_spec:
                for attr_id in self.attr_spec[etype]['attribs']:
                    if self.attr_spec[etype]['attribs'][attr_id].history is False:
                        continue
                    table_name = f"{etype}__{attr_id}"
                    history_params = self.attr_spec[etype]['attribs'][attr_id].history_params
                    t_old = str(datetime.now() - history_params["max_age"])
                    t_redundant = str(datetime.now() - history_params["aggregation_max_age"])
                    self.db.delete_old_datapoints(etype=etype, attr_name=attr_id, t_old=t_old, t_redundant=t_redundant, tag=TAG_REDUNDANT)

            # Check current values for all entities and unset them if expired
            for etype in self.attr_spec:
                entities = self.db.get_entities(etype)
                for eid in entities:
                    updates = {}
                    rec = Record(self.db, etype, eid)
                    for attr_id in self.attr_spec[etype]['attribs']:
                        attr_conf = self.attr_spec[etype]['attribs'][attr_id]
                        attr_exp = f"{attr_id}:exp"
                        attr_c = f"{attr_id}:c"
                        if not attr_conf.history or attr_id not in rec:
                            continue
                        if attr_conf.multi_value:
                            new_val = rec[attr_id]
                            new_exp = rec[attr_exp]
                            new_c = rec[attr_c] if attr_conf.confidence else None
                            for val, exp in zip(rec[attr_id], rec[attr_exp]):
                                if exp < datetime.now():
                                    idx = rec[attr_id].index(val)
                                    del new_val[idx]
                                    del new_exp[idx]
                                    if new_c:
                                        del new_c[idx]
                            updates[attr_id] = new_val
                            updates[attr_exp] = new_exp
                            if new_c:
                                updates[attr_c] = new_c
                        elif rec[attr_exp] < datetime.now():
                            updates[attr_id] = None
                            updates[attr_exp] = None
                            if attr_conf.confidence:
                                updates[attr_c] = None
                    rec.update(updates)
                    rec.push_changes_to_db()

            t_finish = datetime.now()
            next_call = next_call + tick_rate
            if t_finish < next_call:
                self.log.debug(f"History processing finished after {t_finish - t_start}, next call at {next_call}")
                time.sleep((next_call - t_finish).total_seconds())
            else:
                self.log.debug(f"History processing took longer than expected ({t_finish - t_start}), can't stay on schedule")
                next_call = t_finish


def get_historic_value(db, config, etype, eid, attr_id, timestamp):
    attr_spec = config[etype]['attribs'][attr_id]
    t1 = timestamp - attr_spec.history_params['post_validity']
    t2 = timestamp + attr_spec.history_params['pre_validity']
    datapoints = db.get_datapoints_range(etype, attr_id, eid, t1, t2)

    if len(datapoints) < 1:
        return None

    if attr_spec.multi_value is True:
        return set([d['v'] for d in datapoints])

    best = None
    for d in datapoints:
        confidence = extrapolate_confidence(d, timestamp, attr_spec.history_params)
        if best is None or confidence > best[1]:
            best = d['v'], confidence
    return best[0] if best is not None else None


def extrapolate_confidence(datapoint, timestamp, history_params):
    pre_validity = history_params['pre_validity']
    post_validity = history_params['post_validity']
    t1 = datapoint['t1']
    t2 = datapoint['t2']
    base_confidence = datapoint['c']

    if t2 < timestamp:
        distance = timestamp - t2
        multiplier = (1 - (distance / post_validity))
    elif t1 > timestamp:
        distance = t1 - timestamp
        multiplier = (1 - (distance / pre_validity))
    else:
        multiplier = 1
    return base_confidence * multiplier


def csv_union(a, b):
    return ','.join(set(f"{a},{b}".split(sep=',')))


def mergeable(a, b, params):
    res = merge_check[params['aggregation_function_value']](a['v'], b['v'])
    res = res and merge_check[params['aggregation_function_confidence']](a['c'], b['c'])
    return res and merge_check[params['aggregation_function_source']](a['src'], b['src'])


def merge(a, b, history_params):
    a['v'] = merge_apply[history_params['aggregation_function_value']](a['v'], b['v'])
    a['c'] = merge_apply[history_params['aggregation_function_confidence']](a['c'], b['c'])
    a['src'] = merge_apply[history_params['aggregation_function_source']](a['src'], b['src'])
    a['t1'] = str(min(parse_rfc_time(str(a['t1'])), b['t1']))
    a['t2'] = str(max(parse_rfc_time(str(a['t2'])), b['t2']))


merge_check = {
    "keep": lambda a, b: a == b,
    "add": lambda a, b: True,
    "avg": lambda a, b: True,
    "csv_union": lambda a, b: True
}

merge_apply = {
    "keep": lambda a, b: a,
    "add": lambda a, b: a + b,
    "avg": lambda a, b: (a + b) / 2,  # TODO how to compute average?
    "csv_union": lambda a, b: csv_union(a, b)
}
