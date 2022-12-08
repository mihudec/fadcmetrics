import json
from threading import Lock
import requests

class BaseWriter(object):

    def __init__(self, encoding='json'):
        self.lock = Lock()
        self.encoding = encoding

    def to_json(self, data: dict):
        result = None
        try:
            data['@timestamp'] = data['@timestamp'].isoformat()
            result = json.dumps(data)
        except Exception as e:
            pass
        return result

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

    def __init__(self, url: str, encoding='json'):
        self.url = url
        self.session = self.get_session()
        super().__init__(encoding)

    def get_session(self):
        session = requests.Session()
        headers = {
            "Content-Type": "application/json"
        }
        session.headers.update(headers)
        return session

    def write(self, data):
        pass