import json
from socket import MsgFlag
from threading import Lock
import urllib3
import requests
from fadcmetrics.utils.logging import get_logger
from fadcmetrics.config import FileWriterConfig, HttpWriterConfig
from fadcmetrics.exceptions import *
class BaseWriter(object):

    def __init__(self, encoding='json'):
        self.lock = Lock()
        self.encoding = encoding
        self.logger = get_logger(name=self.__class__.__name__)

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

    def to_json(self, data, many: bool = False):
        result = None
        if isinstance(data, list):
            result = [self.prepare_json_output(data=x) for x in data]
        else:
            result = self.prepare_json_output(data=data)
        try:
            result = json.dumps(result)
        except Exception as e:
            self.logger.error(msg=f"ERROR: Exception while serializing to JSON. Data: {data}, Exception: {repr(e)}")
        return result
    
    @classmethod
    def from_config(config):
        raise NotImplemented

    def serialize(self, data):
        if self.encoding == 'json':
            return self.to_json(data=data)

    def write(self, data: dict):
        raise NotImplemented

class StdoutWriter(BaseWriter):

    def write(self, data: dict):
        serial_data = self.serialize(data=data)
        if serial_data is not None:
            with self.lock:
                print(serial_data)

class HttpWriter(BaseWriter):

    def __init__(self, url: str, method: str, encoding='json'):
        self.url = url
        self.method = method
        self.session = self.get_session()
        super().__init__(encoding)

    def get_session(self):
        session = requests.Session()
        headers = {
            "Content-Type": "application/json"
        }
        session.headers.update(headers)
        return session

    def write(self, data, measurement: str = ""):
        if isinstance(data, list):
            data = self.serialize(data=data)
        elif isinstance(data, dict):
            data = self.serialize(data=[data])
        data = {
            "data": data,
            "measurement": measurement
        }
        with self.lock:
            try: 
                self.session.request(method=self.method, url=self.url, data=data)
            except urllib3.exceptions.NewConnectionError as e:
                self.logger.error(msg=f"ERROR: Could not establish connection to {self.url}. {repr(e)}")
                raise HttpWriterException
            except Exception as e:
                self.logger.error(msg=f"ERROR: Unhandled Exception. {repr(e)}")
                raise HttpWriterException

    @classmethod
    def from_config(cls, config: HttpWriterConfig):
        return cls(url=config.url, method=config.method)
