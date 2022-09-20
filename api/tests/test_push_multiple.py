import common


class PushMultiple(common.APITest):

    def test_invalid_payload(self):
        response = self.push_multiple(
            {"type": "test_entity_type", "attr": "test_attr_int", "id": "test_entity_id", "v": 123})
        self.assertEqual(400, response.status_code, "invalid payload (not a list)")

        response = self.push_multiple(["xyz"])
        self.assertEqual(400, response.status_code, "invalid payload (list element is not a dictionary)")

    def test_unknown_entity_type(self):
        response = self.push_multiple([{"type": "xyz", "attr": "test_attr_int", "id": "test_entity_id", "v": 123}])
        self.assertEqual(400, response.status_code)

    def test_unknown_attr_name(self):
        response = self.push_multiple([{"type": "test_entity_type", "id": "test_entity_id", "attr": "xyz", "v": 123}])
        self.assertEqual(400, response.status_code)

    def test_invalid_timestamp(self):
        response = self.push_multiple(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_history", "v": 123, "t1": "xyz"}])
        self.assertEqual(400, response.status_code)

    def test_missing_value(self):
        response = self.push_multiple([{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_int"}])
        self.assertEqual(400, response.status_code)

    def test_missing_value_tag(self):
        response = self.push_multiple([{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_tag"}])
        self.assertEqual(200, response.status_code)

    def helper_test_datatype_value(self, data_type: str, value, expected_code: int):
        response = self.push_multiple(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": f"test_attr_{data_type}", "v": value}])
        self.assertEqual(expected_code, response.status_code)

    def test_data_type_values_valid(self):
        for data_type, valid in common.values["valid"].items():
            for value in valid:
                with self.subTest(data_type=data_type, v=value):
                    self.helper_test_datatype_value(data_type, value=value, expected_code=200)

    def test_data_type_values_invalid(self):
        for data_type, valid in common.values["invalid"].items():
            for value in valid:
                with self.subTest(data_type=data_type, v=value):
                    self.helper_test_datatype_value(data_type, value=value, expected_code=400)

