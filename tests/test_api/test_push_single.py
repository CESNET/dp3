import common


class PushSingle(common.APITest):

    def test_unknown_entity_type(self):
        response = self.push_single("xyz/test_entity_id/test_attr_int", v=123)
        self.assertEqual(400, response.status_code)

    def test_unknown_attr_name(self):
        response = self.push_single("test_entity_type/test_entity_id/xyz", v=123)
        self.assertEqual(400, response.status_code)

    def test_invalid_timestamp(self):
        response = self.push_single("test_entity_type/test_entity_id/test_attr_history", v=123, t1="xyz")
        self.assertEqual(400, response.status_code)

    def test_missing_value(self):
        response = self.push_single("test_entity_type/test_entity_id/test_attr_int")
        self.assertEqual(400, response.status_code)

    def test_missing_value_tag(self):
        response = self.push_single("test_entity_type/test_entity_id/test_attr_tag")
        self.assertEqual(200, response.status_code)

    def helper_test_datatype_value(self, data_type: str, value, expected_code: int):
        response = self.push_single(f"test_entity_type/test_entity_id/test_attr_{data_type}", v=value)
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
