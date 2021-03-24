from dp3.common.utils import parse_rfc_time


class HistoryManager:
    def __init__(self, db, attr_spec):
        # need to synchronize multiple workers (so that aggregations dont interfere with each other) -> singleton?
        self.db = db
        self.attr_spec = attr_spec

    def process_datapoint(self, etype, attr_id, data):
        print("hehexd")
        print(type(data["t1"]))

        pre_validity = 0
        post_validity = 0

        # stage 1
        # primo kolidujici datapointy, nejdriv projdu a checknu jestli to vubec pude ulozit, jestli jo tak to projdu znova a udelam upravy
        candidates = self.db.get_datapoints_range(etype, attr_id, data['eid'], data['t1'], data['t2'])
        print(candidates)
        print("------------")

        # stage 2
        # sloucitelne datapointy
        # jak pricitat a odecitat pre/post validity od timestampu? timedelta
        candidates = self.db.get_datapoints_range(etype, attr_id, data['eid'], data['t1']-post_validity, data['t1'], sort=1)
        print(candidates)
        print("------------")

        candidates = self.db.get_datapoints_range(etype, attr_id, data['eid'], data['t2']+pre_validity, data['t2'], sort=0)
        print(candidates)
        print("------------")

    def process_datapoints_range(self, etype, eid, attr_id, t1, t2):
        pass

    def get_historic_value(self, attr_id, timestamp):
        pass

    def history_management_thread(self):
        pass
