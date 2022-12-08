import time
import datetime
from threading import Thread, Lock, Event, current_thread
from fadcclient.api import FortiAdcApiClient
from fadcmetrics.config import TargetConfig
from fadcmetrics.utils.logging import get_logger
from fadcmetrics.writers import StdoutWriter


class FortiAdcMetricScraper():

    def __init__(self) -> None:
        self.logger = get_logger(name="FADC-Metrics", with_threads=True)
        self.writer = StdoutWriter()
        self.terminate = Event()

    def get_ts(self):
        return datetime.datetime.utcnow()

    def get_client(self, conn_spec: dict):
        client = FortiAdcApiClient(**conn_spec)
        return client

    def get_vs_list(self, client: FortiAdcApiClient):
        response = client.send_request(
            method="GET",
            path='/api/all_vs_info/vs_list',
            params={
                "vdom": "root"
            }
        )
        is_error, error, data = client.handle_response(response=response)
        if not is_error:
            return [x['config']['mkey'] for x in data]


    def get_vs_status(self, client, vs_names: list):
        results = {x: None for x in vs_names}
        for vs_name in vs_names:
            response = client.send_request(
                method="GET",
                path='/api/status_history/vs_status',
                params={
                    "vdom": "root",
                    "vsname": vs_name
                }
            )
            is_error, error, data = client.handle_response(response=response)
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
    
    def get_vs_http(self, client, vs_names: list):
        results = {x: None for x in vs_names}
        for vs_name in vs_names:
            response = client.send_request(
                method="GET",
                path='/api/fortiview/get_vs_http',
                params={
                    "vdom": "root",
                    "vsname": vs_name
                }
            )
            is_error, error, data = client.handle_response(response=response)
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
            vs_names = self.get_vs_list(client=client)
            print(vs_names)
            topics = [x.topic for x in target.scrape_configs]
            while True:
                if self.terminate.is_set():
                    self.logger.info(msg=f"Terminate Event is SET. Terminate Thread {current_thread().name}")
                    return
                if 'vs_status' in topics:
                    vs_status = self.get_vs_status(client=client, vs_names=vs_names)
                    self.enrich_metrics(metrics=vs_status, tags=target.tags)
                    for entry in vs_status:
                        self.writer.write(entry)
                if 'vs_http_stats' in topics:
                    vs_http = self.get_vs_http(client=client, vs_names=vs_names)
                    self.enrich_metrics(metrics=vs_http, tags=target.tags)
                    for entry in vs_http:
                        self.writer.write(entry)
                time.sleep(target.scrape_interval)


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
        print(threads)
        [t.start() for t in threads]
        # Wait while threads are alive
        while any([t.is_alive() for t in threads]):
            try:
                time.sleep(1)
            except KeyboardInterrupt as e:
                self.terminate.set()
                    
