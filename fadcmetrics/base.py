import sys
import time
import datetime
from typing import Dict, List, Pattern
from threading import Thread, Lock, Event, current_thread
from fadcclient.api import FortiAdcApiClient
from fadcmetrics.config import FadcMetricsConfig, TargetConfig
from fadcmetrics.utils.logging import get_logger
from fadcmetrics.exceptions import *
from fadcmetrics.writers import HttpWriter, StdoutWriter


class FadcFortiView():

    def __init__(self, client: FortiAdcApiClient) -> None:
        self.client = client
        self.logger = get_logger(name="FADC-FortiView", with_threads=True)
        self.vs_names = self.get_vs_names()
        self.vs_tree = self.get_vs_tree()

    def get_ts(self):
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    def get_vs_names(self, vdom: str = 'root'):
        vs_names = []
        response = self.client.send_request(
            method="GET",
            path='/api/load_balance_virtual_server/get_vs_name_options',
            params={
                "vdom": "root"
            }
        )
        is_error, error, data = self.client.handle_response(response=response)
        if not is_error:
            if isinstance(data, list):
                vs_names =  data
            else:
                self.logger.error(msg=f"Received unexpected data while getting VS Names. {data=}")
        self.logger.info(f"Discovered VS Names: {vs_names}")
        return vs_names
    
    def get_vs_tree(self, vdom: str = 'root'):
        tree = []
        response = self.client.send_request(
            method="GET",
            path='/api/load_balance_virtual_server/get_trees',
            params={
                "vdom": vdom
            }
        )
        is_error, error, data = self.client.handle_response(response=response)
        if not is_error:
            if isinstance(data, list):
                pass
            else:
                self.logger.error(msg=f"Received unexpected data while getting VS Trees. {data=}")

        def parse_pool(pool_data: dict):
            pool_object = {
                "name": pool_data.get('mkey'),
                "object_type": "realServerPool",
                "children": []
            }
            for rs_data in pool_data.get('children'):
                rs_object = {
                    "name": rs_data.get('real_server_id'),
                    "object_type": "realServer",
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

        return tree
    
    def flatten_tree(self, data, current=None, result=None):
        if current is None:
            current = {}
        if result is None:
            result = []

        if 'name' in data:
            current[data['object_type'] + 'Name'] = data['name']

        if 'children' in data:
            for child in data['children']:
                self.flatten_tree(child, current, result)
        else:
            result.append(current.copy())

        return result
    
    def get_vs_flat_tree(self, vs_name: str):
        vs_tree = [x for x in self.vs_tree if x["name"] == vs_name][0]
        return self.flatten_tree(data=vs_tree)

    def filter_vs_names(self, patterns: List[Pattern]) -> List[str]:
        vs_names = []
        for vs_name in self.vs_names:
            if any([pattern.match(string=vs_name) for pattern in patterns]):
                vs_names.append(vs_name)
        self.logger.info(f"Filtered VS Names: {vs_names}")
        self.vs_names = vs_names


    def get_vs_status(self):
        results = {x: None for x in self.vs_names}
        for vs_name in self.vs_names:
            response = self.client.send_request(
                method="GET",
                path='/api/status_history/vs_status',
                params={
                    "vdom": "root",
                    "vsname": vs_name
                }
            )
            is_error, error, data = self.client.handle_response(response=response)
            if not is_error:
                results[vs_name] = data
                results[vs_name]['@timestamp'] = self.get_ts()
            else:
                self.logger.error(msg=f"Failed to get VS_STATUS for {vs_name}")
        
        results_list = []
        for vs_name, data in results.items():
            if data is None:
                continue
            entry = data
            data['tags'] = {'virtualServerName': vs_name}
            results_list.append(entry)
        return results_list

    def get_vs_http(self):
        results = {x: None for x in self.vs_names}
        for vs_name in self.vs_names:
            response = self.client.send_request(
                method="GET",
                path='/api/fortiview/get_vs_http',
                params={
                    "vdom": "root",
                    "vs": vs_name
                }
            )
            is_error, error, data = self.client.handle_response(response=response)
            if not is_error:
                results[vs_name] = {}
                for key in [f"category_{x}" for x in range(4)]:
                    results[vs_name].update(data[key])
                results[vs_name]['@timestamp'] = self.get_ts()
            else:
                self.logger.error(msg=f"Failed to get VS_HTTP for {vs_name}")
        
        results_list = []
        for vs_name, data in results.items():
            if data is None:
                continue
            entry = data
            data['tags'] = {'virtualServerName': vs_name}
            results_list.append(entry)
        return results_list


class FortiAdcMetricScraper():

    def __init__(self, config: FadcMetricsConfig) -> None:
        self.config = config
        self.logger = get_logger(name="FADC-Metrics", with_threads=True)
        self.writers = self.get_writers()
        self.terminate = Event()
        self.failed = Event()

    def get_ts(self):
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    def get_client(self, conn_spec: dict):
        client = FortiAdcApiClient(**conn_spec)
        return client

    def get_writers(self):
        writers_map = {
            "http": HttpWriter,
            'stdout': StdoutWriter
        }
        writers = []
        for writer_config in self.config.writers:
            writer = writers_map[writer_config.type].from_config(config=writer_config)
            writers.append(writer)
        return writers

    def write(self, data, measurement: str = ""):
        for writer in self.writers:
            try:
                writer.write(data=data, measurement=measurement)
            except Exception as e:
                self.terminate.set()

    def enrich_metrics(self, metrics: dict, tags: dict = None):
        if tags is not None:
            for metric in metrics:
                if metric.get('tags') is None:
                    metric['tags'] = dict()
                metric['tags'].update(tags)

    def worker(self, target: TargetConfig):
        conn_spec = target.dict(include={'base_url', 'username', 'password', 'verify_ssl'})
        self.logger.info(msg=f"Starting metrics scraping on {target.hostname} with scrape_interval={target.scrape_interval}")
        with self.get_client(conn_spec=conn_spec) as client:
            fortiview = FadcFortiView(client=client)
            # Get VirtualServers names
            vs_names = []
            if target.virtual_servers is not None:
                fortiview.filter_vs_names(patterns=target.virtual_servers)
            vs_names = fortiview.vs_names

            if len(vs_names) == 0:
                self.logger.error(msg="Failed to obtain VirtualServers Names.")
                self.terminate.set()
                self.failed.set()
            else:
                self.logger.info(msg=f"Starting to collect VirtualServers: {','.join(vs_names)}")
            topics = [x.topic for x in target.scrape_configs]
            while True:
                if 'vs_status' in topics:
                    vs_status = fortiview.get_vs_status()
                    self.enrich_metrics(metrics=vs_status, tags=target.tags)
                    self.write(data=vs_status, measurement="virtualServerStatus")
                if 'vs_http_stats' in topics:
                    vs_http = fortiview.get_vs_http()
                    self.enrich_metrics(metrics=vs_http, tags=target.tags)
                    self.write(data=vs_http, measurement="virtualServerHttpStats")
                # Number of seconds to sleep in each round
                sleep_interval = 1
                # Number of rounds
                sleep_count = 0
                while (sleep_interval * sleep_count) < target.scrape_interval:
                    if self.terminate.is_set():
                        self.logger.info(msg=f"Terminate Event is SET. Terminate Thread {current_thread().name}")
                        return
                    sleep_count += 1
                    time.sleep(sleep_interval)

    def run(self, targets):
        threads = []
        for i, target in enumerate(targets):
            threads.append(
                Thread(
                    target=self.worker,
                    name=f"T-{i} {target.hostname}",
                    daemon=True,
                    kwargs={"target": target})
            )
        [t.start() for t in threads]
        # Wait while threads are alive
        while any([t.is_alive() for t in threads]):
            try:
                time.sleep(1)
            except KeyboardInterrupt as e:
                self.terminate.set()
        if self.failed.is_set():
            self.logger.error(msg=f"FAILED Event is SET. Exiting with StatusCode=1")
            sys.exit(1)
        else:
            sys.exit(0)
                    
