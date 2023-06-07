import common


class GetHistory(common.APITest):
    def test_unknown_entity_type(self):
        response = self.get_entity("xyz/test_entity_id/get/test_attr_history")
        self.assertEqual(422, response.status_code)

    def test_unknown_attr_name(self):
        response = self.get_entity("test_entity_type/test_entity_id/get/xyz")
        self.assertEqual(422, response.status_code)

    def test_invalid_timestamp(self):
        response = self.get_entity(
            "test_entity_type/test_entity_id/get/test_attr_history", date_from="xyz"
        )
        self.assertEqual(422, response.status_code)

    def test_unknown_entity_id(self):
        response = self.get_entity("test_entity_type/xyz/get/test_attr_history")
        self.assertEqual(200, response.status_code)

    def test_valid_time_intervals(self):
        time_intervals = {
            "<T1, T2>": {"date_from": "2020-01-01T00:00:00", "date_to": "2020-01-01T00:00:00"},
            "<T1, _>": {"date_from": "2020-01-01T00:00:00"},
            "<_, T2>": {"date_to": "2020-01-01T00:00:00"},
            "<_, _>": {},
        }
        for msg, intervals in time_intervals.items():
            with self.subTest(msg=msg):
                response = self.get_entity(
                    "test_entity_type/test_entity_id/get/test_attr_history", **intervals
                )
                self.assertEqual(200, response.status_code)
