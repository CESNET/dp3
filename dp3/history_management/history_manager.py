import datetime
import time
import threading
import hashlib

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
        self.db = db
        self.attr_spec = attr_spec
        self.worker_index = worker_index
        self.num_workers = num_workers
        self.hm_thread = threading.Thread(target=self.history_management_thread, daemon=True)
        self.hm_thread.start()

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
                pass
            else:
                self.split_datapoint(etype, attr_id, d, t1)

        # Merge with non-overlapping datapoints
        pre = self.db.get_datapoints_range(etype, attr_id, data['eid'], str(t1 - aggregation_interval), data['t1'], closed_interval=False, sort=1)
        post = self.db.get_datapoints_range(etype, attr_id, data['eid'], data['t2'], str(t2 + aggregation_interval), closed_interval=False, sort=0)
        for d in pre + post:
            # TODO custom select function? get_datapoints_range() returns all datapoints overlapping with specified interval, we need them to be completely inside the interval here
            if d['t1'] <= t2 or d['t2'] >= t1:
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
        delete_list = []
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

    def get_historic_value(self, etype, eid, attr_id, timestamp):
        attr_spec = self.attr_spec[etype]['attribs'][attr_id]
        t1 = timestamp - attr_spec.history_params['post_validity']
        t2 = timestamp + attr_spec.history_params['pre_validity']
        datapoints = self.db.get_datapoints_range(etype, attr_id, eid, t1, t2)

        if len(datapoints) < 1:
            return None

        if attr_spec.multi_value is True:
            return set([d['v'] for d in datapoints])

        best = None
        for d in datapoints:
            confidence = extrapolate_confidence(d, timestamp, attr_spec.history_params)
            if best is None or confidence > best[1]:
                best = d['v'], confidence
        return best[0]

    def check_hash(self, etype, eid):
        routing_key = HASH(f"{etype}:{eid}") % self.num_workers
        return routing_key == self.worker_index

    def history_management_thread(self):
        # TODO: use apscheduler
        tick_rate = datetime.timedelta(seconds=30)  # TODO add to global config
        next_call = datetime.datetime.now()
        while True:
            for etype in self.attr_spec:
                entities = self.db.get_entities(etype)
                for attr_id in self.attr_spec[etype]['attribs']:
                    if self.attr_spec[etype]['attribs'][attr_id].history is False:
                        continue
                    history_params = self.attr_spec[etype]['attribs'][attr_id].history_params
                    table_name = f"{etype}__{attr_id}"

                    # update attributes current value
                    for eid in entities:
                        if not self.check_hash(etype, eid):
                            continue
                        value = self.get_historic_value(etype, eid, attr_id, datetime.datetime.now())
                        rec = Record(self.db, etype, eid)
                        rec.update({attr_id: value})
                        rec.push_changes_to_db()
                        # print(f"History management thread: Current value of '{etype}/{eid}/{attr_id}' updated to '{value}'")

                    # delete redundant datapoints older than "aggregation_max_age"
                    max_age = history_params["aggregation_max_age"]
                    t2 = str(datetime.datetime.now() - max_age)
                    data = self.db.get_datapoints_range(etype=etype, attr_name=attr_id, t2=t2, tag=TAG_REDUNDANT)
                    for d in data:
                        if not self.check_hash(etype, d['eid']):
                            continue
                        self.db.delete_record(table_name, d['id'])

                    # delete all datapoints older than "max_age"
                    max_age = history_params["max_age"]
                    t2 = str(datetime.datetime.now() - max_age)
                    data = self.db.get_datapoints_range(etype=etype, attr_name=attr_id, t2=t2)
                    for d in data:
                        if not self.check_hash(etype, d['eid']):
                            continue
                        self.db.delete_record(table_name, d['id'])
            next_call = next_call + tick_rate
            time.sleep((next_call - datetime.datetime.now()).total_seconds())


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
