import unittest
import pathlib
import json

from fadcmetrics.client import FadcRestClient
from fadcmetrics.metrics import FadcFortiView

RESOURCE_DIR = pathlib.Path(__file__).parent.joinpath("resources")

def load_json_resource(resource_name: str):
    resource_path = RESOURCE_DIR.joinpath(resource_name)
    resource = None
    with resource_path.open() as f:
        resource = json.load(f)
    return resource


class MockFadcRestClient(FadcRestClient):

    async def get(self, endpoint: str, params: dict = None):
        if endpoint == "/api/load_balance_virtual_server/get_trees":
            return load_json_resource(resource_name="tree_payload_01.json")



class TestFortiView(unittest.TestCase):


    def setUp(self) -> None:
        self.fortiview = FadcFortiView(client=MockFadcRestClient(base_url="https://127.0.0.1", username="foo", password="bar"))
        return super().setUp()
    
    async def test_get_tree_payload(self):
        tree = await self.fortiview.get_vs_tree()



if __name__ == '__main__':
    unittest.main()