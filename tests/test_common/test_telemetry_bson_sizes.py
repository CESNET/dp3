import logging
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

from dp3.common.config import HierarchicalDict
from dp3.database.database import EntityDatabase
from dp3.history_management.telemetry import (
    ATTRIBUTE_BSON_SIZES_TYPE,
    Telemetry,
    TelemetryReader,
)


class FakeCursor(list):
    def sort(self, *_args, **_kwargs):
        return self


class FakeCollection:
    def __init__(self, documents=None):
        self.documents = {repr(document["_id"]): document for document in documents or []}
        self.replacements = []
        self.queries = []
        self.deletions = []

    def replace_one(self, query, record, upsert=False):
        self.replacements.append((query, record, upsert))
        self.documents[repr(record["_id"])] = record

    def delete_many(self, query):
        self.deletions.append(query)
        configured_entities = set(query["entity_type"]["$nin"])
        self.documents = {
            key: record
            for key, record in self.documents.items()
            if record.get("telemetry_type") != query["telemetry_type"]
            or record.get("entity_type") in configured_entities
        }

    def distinct(self, field, query):
        return list(
            {
                record[field]
                for record in self.documents.values()
                if record.get("telemetry_type") == query["telemetry_type"] and field in record
            }
        )

    def find(self, query):
        self.queries.append(query)
        if "telemetry_type" in query:
            records = [
                value
                for value in self.documents.values()
                if value.get("telemetry_type") == query["telemetry_type"]
            ]
        elif "src_t" in query:
            records = [value for value in self.documents.values() if "src_t" in value]
        else:
            records = list(self.documents.values())
        return FakeCursor(records)


class FakeRegistrar:
    def __init__(self):
        self.task_hooks = []
        self.scheduler_jobs = []
        self.one_time_jobs = []

    def register_task_hook(self, hook_type, callback):
        self.task_hooks.append((hook_type, callback))

    def scheduler_register(self, callback, **schedule):
        self.scheduler_jobs.append((callback, schedule))

    def scheduler_register_once(self, callback, **options):
        self.one_time_jobs.append((callback, options))


class FakeTelemetryDatabase:
    def __init__(self, cache, results):
        self.cache = cache
        self.results = results
        self.calls = []

    def get_module_cache(self, _name):
        return self.cache

    def get_attribute_bson_size_stats(self, entity_type):
        self.calls.append(entity_type)
        result = self.results[entity_type]
        if isinstance(result, Exception):
            raise result
        return result


class FakeMasterCollection:
    def __init__(self, result):
        self.result = result
        self.pipeline = None
        self.options = None

    def aggregate(self, pipeline, **options):
        self.pipeline = pipeline
        self.options = options
        return iter(self.result)


class FakeSchema:
    entities = {"E": object()}

    @staticmethod
    def attribs(_entity_type):
        return {
            "plain": object(),
            "observations": object(),
            "timeseries": object(),
            "missing": object(),
        }


class AttributeBsonSizeDatabaseTests(unittest.TestCase):
    def make_database(self, aggregation_result):
        database = object.__new__(EntityDatabase)
        database._db_schema_config = FakeSchema()
        collection = FakeMasterCollection(aggregation_result)
        database._master_col = Mock(return_value=collection)
        return database, collection

    def test_pipeline_sizes_complete_stored_attribute_and_aggregates_statistics(self):
        database, collection = self.make_database(
            [
                {
                    "a0_count": 2,
                    "a0_min": 24,
                    "a0_mean": 29.5,
                    "a0_max": 35,
                    "a0_total": 59,
                    "a1_count": 1,
                    "a1_min": 80,
                    "a1_mean": 80,
                    "a1_max": 80,
                    "a1_total": 80,
                }
            ]
        )

        result = database.get_attribute_bson_size_stats("E")

        self.assertEqual(
            result["plain"], {"count": 2, "min": 24, "mean": 29.5, "max": 35, "total": 59}
        )
        self.assertEqual(
            result["observations"],
            {"count": 1, "min": 80, "mean": 80, "max": 80, "total": 80},
        )
        self.assertEqual(
            result["missing"],
            {"count": 0, "min": None, "mean": None, "max": None, "total": 0},
        )
        projected = collection.pipeline[0]["$project"]["sizes"]
        for index, attribute in enumerate(FakeSchema.attribs("E")):
            expression = projected[f"a{index}"]["$cond"]
            self.assertEqual(expression[0], {"$eq": [{"$type": f"${attribute}"}, "missing"]})
            self.assertEqual(
                expression[2], {"$subtract": [{"$bsonSize": {attribute: f"${attribute}"}}, 5]}
            )
        self.assertTrue(collection.options["allowDiskUse"])

    def test_empty_collection_returns_empty_statistics_for_every_attribute(self):
        database, _collection = self.make_database([])

        result = database.get_attribute_bson_size_stats("E")

        empty = {"count": 0, "min": None, "mean": None, "max": None, "total": 0}
        self.assertEqual(result, dict.fromkeys(FakeSchema.attribs("E"), empty))


class AttributeBsonSizeTelemetryTests(unittest.TestCase):
    def make_platform_config(self, process_index, telemetry_config=None):
        return SimpleNamespace(
            model_spec=SimpleNamespace(entities={"A": object(), "B": object()}),
            config=HierarchicalDict({"telemetry": telemetry_config or {}}),
            process_index=process_index,
            num_processes=2,
        )

    def test_scheduler_is_configurable_and_owned_only_by_process_zero(self):
        cache = FakeCollection()
        database = FakeTelemetryDatabase(cache, {})
        owner_registrar = FakeRegistrar()
        other_registrar = FakeRegistrar()
        schedule = {"minute": "*/20", "second": 7}

        Telemetry(
            database,
            self.make_platform_config(0, {"attribute_bson_size_schedule": schedule}),
            owner_registrar,
        )
        Telemetry(
            database,
            self.make_platform_config(1, {"attribute_bson_size_schedule": schedule}),
            other_registrar,
        )

        owner_jobs = {
            callback.__name__: job_schedule
            for callback, job_schedule in owner_registrar.scheduler_jobs
        }
        other_jobs = {
            callback.__name__: job_schedule
            for callback, job_schedule in other_registrar.scheduler_jobs
        }
        self.assertEqual(owner_jobs["collect_attribute_bson_sizes"]["minute"], "*/20")
        self.assertEqual(owner_jobs["collect_attribute_bson_sizes"]["second"], 7)
        self.assertNotIn("collect_attribute_bson_sizes", other_jobs)
        self.assertIn("sync_to_db", owner_jobs)
        self.assertIn("sync_to_db", other_jobs)
        self.assertEqual(
            [callback.__name__ for callback, _options in owner_registrar.one_time_jobs],
            ["collect_attribute_bson_sizes"],
        )
        self.assertEqual(other_registrar.one_time_jobs, [])

    def test_initial_sweep_is_not_scheduled_when_all_entities_are_cached(self):
        records = [
            {
                "_id": {"telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE, "entity_type": entity},
                "telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE,
                "entity_type": entity,
            }
            for entity in ("A", "B")
        ]
        registrar = FakeRegistrar()

        Telemetry(
            FakeTelemetryDatabase(FakeCollection(records), {}),
            self.make_platform_config(0),
            registrar,
        )

        self.assertEqual(registrar.one_time_jobs, [])

    def test_success_is_atomically_published_and_failure_retains_previous_result(self):
        old_b = {
            "_id": {"telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE, "entity_type": "B"},
            "telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE,
            "entity_type": "B",
            "calculated_at": datetime(2020, 1, 1, tzinfo=UTC),
            "duration_s": 1.0,
            "attributes": {"old": {"count": 1}},
        }
        colliding_source_id = f"{ATTRIBUTE_BSON_SIZES_TYPE}:A"
        source_record = {
            "_id": colliding_source_id,
            "src_t": datetime(2020, 1, 2, tzinfo=UTC),
        }
        cache = FakeCollection([old_b, source_record])
        stats = {"value": {"count": 1, "min": 10, "mean": 10, "max": 10, "total": 10}}
        database = FakeTelemetryDatabase(cache, {"A": stats, "B": RuntimeError("scan failed")})
        telemetry = Telemetry(database, self.make_platform_config(0), FakeRegistrar())

        with self.assertLogs("Telemetry", logging.ERROR):
            telemetry.collect_attribute_bson_sizes()

        self.assertEqual(database.calls, ["A", "B"])
        self.assertEqual(len(cache.replacements), 1)
        replacement = cache.replacements[0][1]
        self.assertEqual(replacement["entity_type"], "A")
        self.assertEqual(replacement["attributes"], stats)
        self.assertIsInstance(replacement["calculated_at"], datetime)
        self.assertGreaterEqual(replacement["duration_s"], 0)
        self.assertIs(cache.documents[repr(old_b["_id"])], old_b)
        self.assertIs(cache.documents[repr(colliding_source_id)], source_record)
        self.assertEqual(
            cache.deletions,
            [
                {
                    "telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE,
                    "entity_type": {"$nin": ["A", "B"]},
                }
            ],
        )

    def test_reader_uses_only_cached_records(self):
        calculated_at = datetime(2024, 1, 2, tzinfo=UTC)
        record = {
            "_id": {"telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE, "entity_type": "A"},
            "telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE,
            "entity_type": "A",
            "calculated_at": calculated_at,
            "duration_s": 2.5,
            "attributes": {
                "value": {"count": 0, "min": None, "mean": None, "max": None, "total": 0}
            },
        }
        cache = FakeCollection([record, {"_id": "source", "src_t": calculated_at}])
        database = Mock()
        database.get_module_cache.return_value = cache
        reader = TelemetryReader(database, "app", 1, {})

        result = reader.get_attribute_bson_sizes()

        self.assertEqual(result["A"]["calculated_at"], calculated_at)
        self.assertEqual(result["A"]["attributes"], record["attributes"])
        database.get_attribute_bson_size_stats.assert_not_called()
        self.assertEqual(cache.queries[-1], {"telemetry_type": ATTRIBUTE_BSON_SIZES_TYPE})
        self.assertEqual(reader.get_sources_validity(), {"source": calculated_at})


if __name__ == "__main__":
    unittest.main()
