import unittest
from types import SimpleNamespace

from dp3.common.attrspec import AttrType
from dp3.database.database import EntityDatabase


class RecordingCollection:
    def __init__(self):
        self.batches = []

    def bulk_write(self, updates):
        self.batches.append(list(updates))


class TestDeleteManyLinkDatapoints(unittest.TestCase):
    def test_updates_are_batched_once_per_entity_type(self):
        collections = {"A": RecordingCollection(), "B": RecordingCollection()}
        attr_types = {
            ("A", "observed_bs"): AttrType.OBSERVATIONS,
            ("A", "plain_b"): AttrType.PLAIN,
            ("B", "observed_as"): AttrType.OBSERVATIONS,
        }

        database = EntityDatabase.__new__(EntityDatabase)
        database._master_col = collections.__getitem__
        database._db_schema_config = SimpleNamespace(
            attr=lambda etype, attr: SimpleNamespace(t=attr_types[etype, attr])
        )

        database.delete_many_link_dps(
            etypes=["A", "A", "B"],
            affected_eids=[["a1"], ["a2"], ["b1"]],
            attr_names=["observed_bs", "plain_b", "observed_as"],
            eids_to=[["b1"], ["b2"], ["a1"]],
        )

        self.assertEqual([len(batch) for batch in collections["A"].batches], [2])
        self.assertEqual([len(batch) for batch in collections["B"].batches], [1])


if __name__ == "__main__":
    unittest.main()
