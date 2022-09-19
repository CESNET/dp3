import common


class PushMultiple(common.APITest):

    def test_invalid_payload(self):
        response = self.helper_send_to_multiple(
            {"type": "test_entity_type", "attr": "test_attr_int", "id": "test_entity_id", "v": 123})
        self.assertEqual(400, response.status_code, "invalid payload (not a list)")

        response = self.helper_send_to_multiple(["xyz"])
        self.assertEqual(400, response.status_code, "invalid payload (list element is not a dictionary)")

    def test_unknown_entity_type(self):
        response = self.helper_send_to_multiple(
            [{"type": "xyz", "attr": "test_attr_int", "id": "test_entity_id", "v": 123}])
        self.assertEqual(400, response.status_code)

    def test_unknown_attr_name(self):
        response = self.helper_send_to_multiple(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": "xyz", "v": 123}])
        self.assertEqual(400, response.status_code)

    def test_invalid_timestamp(self):
        response = self.helper_send_to_multiple(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_history", "v": 123, "t1": "xyz"}])
        self.assertEqual(400, response.status_code)

    def test_missing_value(self):
        response = self.helper_send_to_multiple(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_int"}])
        self.assertEqual(400, response.status_code)

    def test_missing_value_tag(self):
        response = self.helper_send_to_multiple(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_tag"}])
        self.assertEqual(200, response.status_code)

    def helper_test_datatype_values(self, data_type: str, values: list, expected_code: int):
        for v in values:
            response = self.helper_send_to_multiple(
                [{"type": "test_entity_type", "id": "test_entity_id", "attr": f"test_attr_{data_type}", "v": v}])
            self.assertEqual(expected_code, response.status_code, "{data_type} | v={v}")

    def test_data_type_values_valid(self):
        for data_type in common.data_types:
            with self.subTest(data_type=data_type):
                valid = common.values["valid"][data_type]
                self.helper_test_datatype_values(data_type, values=valid, expected_code=200)

    def test_data_type_values_invalid(self):
        for data_type in common.data_types:
            with self.subTest(data_type=data_type):
                invalid = common.values["invalid"][data_type]
                self.helper_test_datatype_values(data_type, values=invalid, expected_code=400)
