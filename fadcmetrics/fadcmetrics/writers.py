import json
import asyncio
import aiohttp

from fadcmetrics.utils.logging import get_logger
from fadcmetrics.config import FileWriterConfig, HttpWriterConfig
from fadcmetrics.exceptions import *


class BaseWriter(object):

    def __init__(self, encoding='json'):
        self.encoding = encoding
        self.logger = get_logger(name=self.__class__.__name__)

    async def initialize(self):
        pass

    async def close(self):
        pass

    def prepare_json_output(self, data) -> dict:
        try:
            # data['@timestamp'] = data['@timestamp'].isoformat()
            data['@timestamp'] = int(data['@timestamp'].timestamp())
            # if data.get('tags') is not None:
            #     tags = data.pop('tags')
            #     data.update(tags)
        except Exception as e:
            self.logger.error(msg=f"ERROR: Exception while preparing output for JSON. Data: {data}, Exception: {repr(e)}")

        return data

    def prepare_metrics_data(self, metric: dict) -> dict:
        try:
            # data['@timestamp'] = data['@timestamp'].isoformat()
            metric['@timestamp'] = int(metric['@timestamp'].timestamp())
            # if data.get('tags') is not None:
            #     tags = data.pop('tags')
            #     data.update(tags)
        except Exception as e:
            self.logger.error(msg=f"ERROR: Exception while preparing metric. Data: {metric}, Exception: {repr(e)}")

        return metric

    def to_json(self, data):
        result = None
        try:
            if isinstance(data, dict):
                metrics = data.get('metrics')
                if isinstance(metrics, list):
                    metrics = [self.prepare_metrics_data(metric=x) for x in metrics]
                    data['metrics'] = metrics
                result = json.dumps(data)
            else:
                raise ValueError(f"Unexpected data format: {data}")
        except Exception as e:
            self.logger.error(msg=f"ERROR: Exception while serializing to JSON. Data: {data}, Exception: {repr(e)}")
        return result
    
    @classmethod
    def from_config(cls, config):
        raise NotImplemented

    def serialize(self, data):
        if self.encoding == 'json':
            return self.to_json(data=data)

    def write(self, data: dict):
        raise NotImplemented

class StdoutWriter(BaseWriter):

    async def write(self, data: dict, measurement: str = ""):
        data = {
            "metrics": data,
            "measurement": measurement
        }
        serial_data = self.serialize(data=data)
        if serial_data is not None:
            print(serial_data)

    @classmethod
    def from_config(cls, config):
        return cls()

class HttpWriter(BaseWriter):

    def __init__(self, url: str, method: str, encoding='json'):
        self.url = str(url)
        self.method = method
        self.verify_ssl = False
        self._session = None
        super().__init__(encoding)

    async def initialize(self):
        session_headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Cache-Control": "no-cache"
            }
        cookie_jar = aiohttp.CookieJar(unsafe=True) # This is because aiohttp does not accept Cookies from IP addresses (such as https://127.0.0.1) by default
        session = aiohttp.ClientSession(headers=session_headers, connector=aiohttp.TCPConnector(ssl=self.verify_ssl), cookie_jar=cookie_jar)
        self._session = session

    async def close(self):
        await self._session.close()


    async def write(self, data, measurement: str = ""):
        data = {
            "metrics": data,
            "measurement": measurement
        }
        data = self.serialize(data=data)
        
        if not isinstance(data, str):
            raise ValueError(f"Error while writing data. Expected JSON str, got {type(data)}")
        try: 
            if self.method.lower() == "post":
                response = await self._session.post(url=self.url, data=data)
        except Exception as e:
            self.logger.error(msg=f"ERROR: Unhandled Exception. {repr(e)}")
            raise HttpWriterException

    @classmethod
    def from_config(cls, config: HttpWriterConfig):
        return cls(url=config.url, method=config.method)


