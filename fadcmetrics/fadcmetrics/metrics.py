import asyncio
import aiohttp
import json
import datetime
import time
from functools import wraps
from copy import deepcopy

from fadcmetrics.utils.logging import get_logger
from fadcmetrics.exceptions import *
from fadcmetrics.client import RestClient, FadcRestClient
from fadcmetrics.config import *
from fadcmetrics.writers import *

from typing import List, Dict, Union


import time
import asyncio
from functools import wraps


def dump_data(filename: str, data):
    with pathlib.Path(__file__).parent.parent.joinpath("data_dumps").joinpath(filename).open(mode='w') as f:
        json.dump(obj=data, fp=f, indent=2)

class FadcFortiView:


    HEALTH_STATUS_MAP = {
            "HEALTHY": 0,
            "DISABLED": 1,
            "UNHEALTHY": 2,
            "DOWN": 3
    }

    def __init__(self, client: FadcRestClient, hostname: str = "UNSPECIFIED", verbosity: int = 4):
        self.client: RestClient = client
        self.verbosity = verbosity
        self.hostname = hostname
        self.logger = get_logger(name=f"{self.__class__.__name__}: {self.hostname}", verbosity=verbosity)
        self.tree_list = None
        self.tree_dict = None
        self.flat_trees = None

    def get_ts(self):
        return datetime.datetime.now(datetime.timezone.utc)

    def map_health_status(self, status: str) -> int:
        return self.HEALTH_STATUS_MAP.get(status, 5)

    async def get_vs_tree(self, vdom: str = 'root'):
        self.logger.info(f"Fetching tree...")
        tree = []
        endpoint = "/api/load_balance_virtual_server/get_trees"
        params = {"vdom": vdom}
        data = await self.client.get(endpoint=endpoint, params=params)        
        if data is None:
            self.tree_list = {}
            return  
        def parse_pool(pool_data: dict):
            pool_object = {
                "name": pool_data.get('mkey'),
                "object_type": "realServerPool",
                "current_status": pool_data.get("current-status"),
                "children": []
            }
            for rs_data in pool_data.get('children'):
                rs_object = {
                    "name": rs_data.get('real_server_id'),
                    "object_type": "realServer",
                    "current_status": rs_data.get("current-status"),
                    "member_id": rs_data.get('mkey'),
                    "address": rs_data.get('address'),
                    "port": rs_data.get('port'),
                }
                pool_object["children"].append(rs_object)
            return pool_object

        for vs_data in data:
            cr_enabled = True if vs_data.get('content-routing') == 'enable' else False
            
            vs_object = {
                "name": vs_data.get('mkey'),
                "object_type": "virtualServer",
                "current_status": vs_data.get("current-status"),
                "children": []
            }

            if cr_enabled:
                for cr_data in vs_data.get('children'):
                    cr_object = {
                        "name": cr_data.get('mkey'),
                        "object_type": "contentRouting",
                        "children": []
                    }
                    for pool_data in cr_data.get('children'):
                        pool_object = parse_pool(pool_data=pool_data)
                        cr_object["children"].append(pool_object)

                    vs_object["children"].append(cr_object)
            else:
                cr_object = {
                    "name": "N/A",
                    "object_type": "contentRouting",
                    "children": []
                }
                for pool_data in vs_data.get('children'):
                    pool_object = parse_pool(pool_data=pool_data)
                    cr_object["children"].append(pool_object)
                vs_object["children"].append(cr_object)
            tree.append(vs_object)

        self.tree_list = tree
        self.tree_dict = self.list_to_nested_dict(data=deepcopy(tree))
        self.flat_trees = {x:self.flatten_vs_trees(vs_name=x) for x in self.get_vs_names()}
        self.logger.info(f"Discovered {len(tree)} Virtual Servers")
        # dump_data("tree_list.json", self.tree_list)
        # dump_data("tree_dict.json", self.tree_dict)
        # dump_data("flat_trees.json", self.flat_trees)

    
    def flatten_tree(self, data, current=None, result=None):
        if current is None:
            current = {}
        if result is None:
            result = []

        if 'name' in data:
            current[data['object_type'] + 'Name'] = data['name']
        if 'member_id' in data:
            current['poolMemberId'] = data['member_id']

        if 'children' in data:
            for child in data['children']:
                self.flatten_tree(child, current, result)
        else:
            result.append(current.copy())

        return result
    
    def get_vs_names(self):
        return [x.get("name") for x in self.tree_list]

    def flatten_vs_trees(self, vs_name: str):
        vs_tree = [x for x in self.tree_list if x["name"] == vs_name][0]
        return self.flatten_tree(data=vs_tree)

    def get_vs_flat_tree(self, vs_name: str):
        vs_tree = [x for x in self.tree_list if x["name"] == vs_name][0]
        return self.flatten_tree(data=vs_tree)
    
    def list_to_nested_dict(self, data):
        nested_dict = {}
        for item in data:
            name = item["name"]
            nested_dict[name] = item
            if "children" in item:
                nested_dict[name]["children"] = self.list_to_nested_dict(item["children"])
        return nested_dict
    
    async def get_vs_status(self):
        vs_names = self.get_vs_names()
        vs_health_statuses = {x["name"]: self.map_health_status(x["current_status"]) for x in self.tree_list}
        tag_sets = [{"virtualServerName": x} for x in vs_names]
        sorted(vs_names)
        queries = []
        for tag_set in tag_sets:
            queries.append(
                {
                    "endpoint": "/api/status_history/vs_status", 
                    "params": {
                        "vdom": "root",
                        "vsname": tag_set.get("virtualServerName")
                        }
                }
            )
        responses = await self.client.bulk_get(requests=queries)
        results_list = []
        timestamp = self.get_ts()
        for tag_set, response in zip(tag_sets, responses):
            entry = dict(response)
            entry.update({"health": vs_health_statuses.get(tag_set["virtualServerName"])})
            entry.update({"tags": tag_set})
            entry.update({"@timestamp": timestamp})
            results_list.append(entry)
        return results_list

    async def get_vs_http(self):
        vs_names = self.get_vs_names()
        tag_sets = [{"virtualServerName": x} for x in vs_names]
        sorted(vs_names)
        queries = []
        for tag_set in tag_sets:
            queries.append(
                {
                    "endpoint": "/api/fortiview/get_vs_http", 
                    "params": {
                        "vdom": "root",
                        "vs": tag_set.get("virtualServerName")
                        }
                }
            )
        responses = await self.client.bulk_get(requests=queries)
        results_list = []
        timestamp = self.get_ts()
        for tag_set, response in zip(tag_sets, responses):
            entry = {}
            for key in [f"category_{x}" for x in range(4)]:
                entry.update(response[key])
            entry.update({"tags": tag_set})
            entry.update({"@timestamp": timestamp})
            results_list.append(entry)
        return results_list
    
    async def get_rs_status(self):
        vs_names = self.get_vs_names()
        sorted(vs_names)
        tag_sets = []
        for vs_name in vs_names:
            flat_tree = self.flat_trees.get(vs_name)
            for branch in flat_tree:
                tag_set = branch
                tag_sets.append(tag_set)

        queries = []
        rs_health_statuses = {}
        for tag_set in tag_sets:
            vs_name = tag_set.get("virtualServerName")
            cr_name = tag_set.get("contentRoutingName")
            pool_name = tag_set.get("realServerPoolName")
            rs_name = tag_set.get("realServerName")
            rs_member_id = tag_set.get("poolMemberId")
            if pool_name not in rs_health_statuses.keys():
                rs_health_statuses[pool_name] = {}
            rs_health_statuses[pool_name][rs_name] = self.tree_dict[vs_name]["children"][cr_name]["children"][pool_name]["children"][rs_name]["current_status"]

            queries.append(
                {
                    "endpoint": "/api/status_history/rs_status", 
                    "params": {
                        "vdom": "root",
                        "vsname": vs_name,
                        "crname": cr_name if cr_name != "N/A" else "",
                        "poolname": pool_name,
                        "member": rs_member_id,
                    }
                }
            )
        responses = await self.client.bulk_get(requests=queries)
        results_list = []
        timestamp = self.get_ts()
        for tag_set, response in zip(tag_sets, responses):
            entry = dict(response)
            entry.update({"health": self.map_health_status(rs_health_statuses[tag_set.get("realServerPoolName")][tag_set.get("realServerName")])})
            entry.update({"tags": tag_set})
            entry.update({"@timestamp": timestamp})
            results_list.append(entry)
        return results_list


class FadcMetricsScraper:

    

    def __init__(self, config: FadcMetricsConfig, verbosity: int = 4) -> None:
        self.config = config
        self.writers = None
        self.verbosity = verbosity
        self.logger = get_logger(name=self.__class__.__name__, verbosity=self.verbosity)

    async def get_writers(self):
        writers_map = {
            'http': HttpWriter,
            'stdout': StdoutWriter
        }
        writers = []
        for writer_config in self.config.writers:
            writer = writers_map[writer_config.type].from_config(config=writer_config)
            await writer.initialize()
            writers.append(writer)
        self.writers = writers

    async def close_writers(self):
        self.logger.info("Closing writers...")
        for writer in self.writers:
            await writer.close()
        self.logger.info("Writers closed.")
    
    def enrich_metrics(self, metrics: dict, tags: dict = None):
        if tags is not None:
            for metric in metrics:
                if metric.get('tags') is None:
                    metric['tags'] = dict()
                metric['tags'].update(tags)

    async def write(self, data, measurement: str = ""):
        for writer in self.writers:
            try:
                await writer.write(data=data, measurement=measurement)
                self.logger.debug(f"Wrote metrics for {measurement=} to {writer.__class__.__name__}")
            except Exception as e:
                # TODO: Exception Handling
                raise

    async def worker(self, target: TargetConfig):
        hostname = target.hostname
        topics = [x.topic for x in target.scrape_configs]
        delay = 5
        # Loop for handling backoff
        while True:
            await self.get_writers()
            try:
                async with FadcRestClient(base_url=target.base_url, username=target.username, password=target.password, verbosity=self.verbosity) as client:
                    await client.initialize()
                    fortiview = FadcFortiView(client=client, hostname=target.hostname, verbosity=self.verbosity)
                    # Loop for metrics scraping
                    while True:
                        await fortiview.get_vs_tree()
                        if "vs_http_stats" in topics:
                            vs_http_stats = await fortiview.get_vs_http()
                            self.enrich_metrics(metrics=vs_http_stats, tags=target.tags)
                            await self.write(data=vs_http_stats, measurement="virtualServerHttpStats")
                        if "vs_status" in topics:
                            vs_status = await fortiview.get_vs_status()
                            self.enrich_metrics(metrics=vs_status, tags=target.tags)
                            await self.write(data=vs_status, measurement="virtualServerStatus")
                        if "rs_status" in topics:
                            rs_status = await fortiview.get_rs_status()
                            self.enrich_metrics(metrics=rs_status, tags=target.tags)
                            await self.write(data=rs_status, measurement="realServerStatus")

                        await asyncio.sleep(target.scrape_interval)
            except asyncio.CancelledError as e:
                self.logger.info("Scraper canceled.")
                await self.close_writers()
                return
            except aiohttp.ClientConnectionError as e:
                self.logger.error(f"Failed to connect to target: {target.base_url}")
                self.logger.error(f"Backoff for {delay}")
                await asyncio.sleep(delay=delay)
                delay *= 2
            except Exception as e:
                self.logger.error(f"Unexpected exception while retrieving metrics: {repr(e)}")
                await self.close_writers()
                raise


    async def run(self):
        tasks = [self.worker(target) for target in self.config.targets]
        await asyncio.gather(*tasks)


def main():
    config = get_config()
    metrics_scraper = FadcMetricsScraper(config=config, verbosity=config.verbosity)
    try:
        asyncio.run(metrics_scraper.run())
    except KeyboardInterrupt as e:
        print("Keyboard Interrupt - Exiting")
        time.sleep(5)

if __name__ == '__main__':
    main()
