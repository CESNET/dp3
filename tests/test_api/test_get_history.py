import common


class GetHistory(common.APITest):
    def test_unknown_entity_type(self):
        response = self.get_request("xyz/test_entity_id/test_attr_history/history")
        self.assertEqual(400, response.status_code)

    def test_unknown_attr_name(self):
        response = self.get_request("test_entity_type/test_entity_id/xyz/history")
        self.assertEqual(400, response.status_code)

    def test_invalid_timestamp(self):
        response = self.get_request(
            "test_entity_type/test_entity_id/test_attr_history/history", t1="xyz"
        )
        self.assertEqual(400, response.status_code)

    def test_unknown_entity_id(self):
        response = self.get_request("test_entity_type/xyz/test_attr_history/history")
        self.assertEqual(200, response.status_code)

    def test_valid_time_intervals(self):
        time_intervals = {
            "<T1, T2>": {"t1": "2020-01-01T00:00:00", "t2": "2020-01-01T00:00:00"},
            "<T1, _>": {"t1": "2020-01-01T00:00:00"},
            "<_, T2>": {"t2": "2020-01-01T00:00:00"},
            "<_, _>": {},
        }
        for msg, intervals in time_intervals.items():
            with self.subTest(msg=msg):
                response = self.get_request(
                    "test_entity_type/test_entity_id/test_attr_history/history", **intervals
                )
                self.assertEqual(200, response.status_code)

    def test_invalid_time_interval(self):
        response = self.get_request(
            "test_entity_type/test_entity_id/test_attr_history/history",
            t1="2020-01-01T00:00:01",
            t2="2020-01-01T00:00:00",
        )
        self.assertEqual(400, response.status_code, "Should not be valid, T1 > T2")
