import common


class PushSingle(common.APITest):

    def test_unknown_entity_type(self):
        response = self.helper_send_to_single("xyz/test_entity_id/test_attr_int", v=123)
        self.assertEqual(400, response.status_code)

    def test_unknown_attr_name(self):
        response = self.helper_send_to_single("test_entity_type/test_entity_id/xyz", v=123)
        self.assertEqual(400, response.status_code)

    def test_invalid_timestamp(self):
        response = self.helper_send_to_single("test_entity_type/test_entity_id/test_attr_history", v=123, t1="xyz")
        self.assertEqual(400, response.status_code)

    def test_missing_value(self):
        response = self.helper_send_to_single("test_entity_type/test_entity_id/test_attr_int")
        self.assertEqual(400, response.status_code)

    def test_missing_value_tag(self):
        response = self.helper_send_to_single("test_entity_type/test_entity_id/test_attr_tag")
        self.assertEqual(200, response.status_code)

    def helper_test_datatype_values(self, data_type: str, values: list, expected_code: int):
        for v in values:
            response = self.helper_send_to_single(f"test_entity_type/test_entity_id/test_attr_{data_type}", v=v)
            self.assertEqual(expected_code, response.status_code)

    def test_data_type_values_valid(self):
        for data_type in common.data_types:
            with self.subTest(data_type=data_type):
                valid = common.values["valid"][data_type]
                self.helper_test_datatype_values(data_type, valid, 200)

    def test_data_type_values_invalid(self):
        for data_type in common.data_types:
            with self.subTest(data_type=data_type):
                invalid = common.values["invalid"][data_type]
                self.helper_test_datatype_values(data_type, invalid, 400)
