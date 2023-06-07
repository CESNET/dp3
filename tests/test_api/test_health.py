import common


class HealthCheck(common.APITest):
    def test_api_up(self):
        response = self.get_request("")
        self.assertEqual(200, response.status_code)
