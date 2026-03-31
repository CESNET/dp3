import unittest
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, Field

from dp3.common.types import AwareDatetime, T2Datetime


class _AwareModel(BaseModel):
    dt: AwareDatetime


class _T2Model(BaseModel):
    t1: AwareDatetime
    t2: T2Datetime = Field(None, validate_default=True)


class TestAwareDatetime(unittest.TestCase):
    def test_naive_datetime_defaults_to_utc(self):
        model = _AwareModel(dt="2024-01-01T10:00:00")
        self.assertEqual(model.dt.tzinfo, timezone.utc)

    def test_existing_timezone_is_preserved(self):
        cest_timezone = timezone(timedelta(hours=2), "CEST")
        aware = datetime(2024, 1, 1, 10, 0, tzinfo=cest_timezone)
        model = _AwareModel(dt=aware)
        self.assertEqual(model.dt.tzinfo, cest_timezone)

    def test_t2_datetime_inherits_timezone_when_missing(self):
        model = _T2Model(t1="2024-01-01T00:00:00")
        self.assertIsNotNone(model.t2)
        self.assertEqual(model.t1.tzinfo, timezone.utc)
        self.assertEqual(model.t2.tzinfo, timezone.utc)
        self.assertEqual(model.t2, model.t1)
