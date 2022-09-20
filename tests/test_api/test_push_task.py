import common


class PushTask(common.APITest):
    base_task = {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
    }

    def test_unknown_entity_type(self):
        response = self.push_task({
            "etype": "xyz",
            "ekey": "test_entity_id"
        })
        self.assertEqual(400, response.status_code)

    def test_invalid_attr_updates(self):
        invalid_attr_updates = {
            "not a list": {"attr": "test_attr_int", "op": "set", "val": 1},
            "list element is not a dictionary": ["xyz"],
            "missing 'op' field": [{"attr": "test_attr_int", "val": 1}],
            "unknown attr name": [{"attr": "xyz", "op": "set", "val": 1}],
            "unknown operation": [{"attr": "test_attr_int", "op": "xyz", "val": 1}],
            "invalid value": [{"attr": "test_attr_int", "op": "set", "val": "xyz"}],
        }
        for description, invalid_value in invalid_attr_updates.items():
            with self.subTest(msg=f"invalid attr_updates ({description})"):
                response = self.push_task({**self.base_task, "attr_updates": invalid_value})
                self.assertEqual(400, response.status_code, f"invalid attr_updates ({description})")

    def test_invalid_data_points(self):
        invalid_data_points = {
            "not a list": {"attr": "test_attr_int", "v": 1, "t1": "2020-01-01T00:00:00"},
            "list element is not a dictionary": ["xyz"],
            "missing 'attr' field": [{"v": 1, "t1": "2020-01-01T00:00:00"}],
            "invalid timestamp": [{"attr": "test_attr_int", "v": 1, "t1": "xyz"}],
            "invalid value": [{"attr": "test_attr_int", "v": "xyz", "t1": "2020-01-01T00:00:00"}],
            "invalid confidence": [{"attr": "test_attr_int", "v": 1, "t1": "2020-01-01T00:00:00", "c": "xyz"}],
        }
        for description, invalid_value in invalid_data_points.items():
            with self.subTest(msg=f"invalid data_points ({description})"):
                response = self.push_task({**self.base_task, "data_points": invalid_value})
                self.assertEqual(400, response.status_code, f"invalid data_points ({description})")

    def test_invalid_events(self):
        invalid_events = {
            "not a list": "xyz",
            "list element is not a string": [123],
        }
        for description, invalid_value in invalid_events.items():
            with self.subTest(msg=f"invalid events ({description})", events=invalid_value):
                response = self.push_task({**self.base_task, "events": invalid_value})
                self.assertEqual(400, response.status_code, f"invalid events ({description})")

    def test_invalid_create(self):
        response = self.push_task({**self.base_task, "create": "xyz"})
        self.assertEqual(400, response.status_code)

    def test_invalid_delete(self):
        response = self.push_task({**self.base_task, "delete": "xyz"})
        self.assertEqual(400, response.status_code)

    def test_invalid_src(self):
        response = self.push_task({**self.base_task, "src": 123})
        self.assertEqual(400, response.status_code)

    def test_invalid_tags(self):
        response = self.push_task({**self.base_task, "tags": "xyz"})
        self.assertEqual(400, response.status_code)
