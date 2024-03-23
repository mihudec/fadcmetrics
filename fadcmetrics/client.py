import asyncio
import aiohttp
import json
import datetime
import time
from functools import wraps

from fadcmetrics.utils.logging import get_logger
from fadcmetrics.exceptions import *

from typing import List, Dict, Union



def timer(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        start_time = time.time()
        result = await func(self, *args, **kwargs)
        end_time = time.time()
        message = f"Execution time for {self.__class__.__name__}.{func.__name__}: {end_time - start_time} seconds"
        if hasattr(self, 'logger'):
            self.logger.debug(message)
        else:
            print(message)
        return result
    return wrapper
    

class RestClient:
    def __init__(self, base_url: str, username: str, password: str, verify_ssl: bool = False, verbosity: int = 4) -> None:
        self.base_url = base_url
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.verbosity = verbosity
        self.logger = get_logger(name=self.__class__.__name__, verbosity=self.verbosity, with_threads=False)
        self.logger.info(msg="Initializing Fortinet REST API Client")
        self._session = None
        self.semaphore = asyncio.Semaphore(1)


    async def initialize(self):
        self.session_headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Cache-Control": "no-cache"
            }
        cookie_jar = aiohttp.CookieJar(unsafe=True) # This is because aiohttp does not accept Cookies from IP addresses (such as https://127.0.0.1) by default
        session = aiohttp.ClientSession(headers=self.session_headers, connector=aiohttp.TCPConnector(ssl=self.verify_ssl), cookie_jar=cookie_jar)
        self._session = session
        
    async def handle_response(self, response_data: dict):
        is_error, error, data = (None, None, None)
        data = response_data.get("payload")
        if data is None:
            is_error = True
        else:
            if isinstance(data, int) and data < 0:
                is_error = True
                error = self.get_err_msg(err_id=data)
                self.logger.error(msg=f"Error Response: Code: {data} Msg: {error}")
            else:
                is_error = False
        return is_error, error, data
    

    def retry(max_retries=1):
        def inner(func):
            async def wrapper(self, *args, **kwargs):
                retries = 0
                while retries <= max_retries:
                    try:
                        status_code, response_data = await func(self, *args, **kwargs)
                        if status_code != 401: 
                            return status_code, response_data
                        else:
                            self.logger.info(f"Unauthorized - Retrying {retries}/{max_retries}")
                            await self.authenticate()
                    except AuthenticationFailed as e:
                        # if retries == max_retries:
                        raise
                        # return status_code, response_data
                    finally:
                        retries += 1

                return status_code, response_data

            async def async_wrapper(*args, **kwargs):
                return await wrapper(*args, **kwargs)

            return async_wrapper

        return inner
    
    @retry()
    async def _get(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        self.logger.debug(f"GET: {url} {params=}")
        async with self._session.get(url, params=params) as response:
            response_data = await response.json()
            return response.status, response_data


    @retry()
    async def _post(self, endpoint, data=None):
        url = f"{self.base_url}{endpoint}"
        async with self._session.post(url, json=data) as response:
            response_data = await response.json()
            return response.status, response_data

    
    async def get(self, endpoint: str, params: dict = None):
        response_status, response_data = await self._get(endpoint=endpoint, params=params)
        is_error, error, data = await self.handle_response(response_data=response_data)
        return data
    
    @timer
    async def bulk_get(self, requests):
        tasks = []
        for req in requests:
            tasks.append(self.get(req["endpoint"], req.get("params")))
        responses = None
        responses = await asyncio.gather(*tasks)
        return responses

    async def __aenter__(self):
        # await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.logger.info(f"Closing client session for {self.base_url}")
        await self._session.close()
       

class FadcRestClient(RestClient):

    def __init__(self, base_url: str, username: str, password: str, verify_ssl: bool = False, verbosity: int = 4) -> None:
        super().__init__(base_url, username, password, verify_ssl, verbosity)


    async def initialize(self):
        try:
            self.session_headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Cache-Control": "no-cache"
                }
            cookie_jar = aiohttp.CookieJar(unsafe=True) # This is because aiohttp does not accept Cookies from IP addresses (such as https://127.0.0.1) by default
            session = aiohttp.ClientSession(headers=self.session_headers, connector=aiohttp.TCPConnector(ssl=self.verify_ssl), cookie_jar=cookie_jar)
            self._session = session
            auth_success = await self.authenticate()
            if auth_success:
                self.logger.info(f"Authentication successful")
            else:
                self.logger.error("Failed to authenticate.")
        except Exception as e:
            self.logger.error(f"Exception while initializig client: {repr(e)}")
            raise



    async def authenticate(self):
        auth_url=f"{self.base_url}/api/user/login"
        auth_payload=json.dumps({
            "username": self.username,
            "password": self.password
        })

        async with self._session.post(url=auth_url, data=auth_payload) as response:
            response_data = None
            auth_success = False
            try:
                response_status = response.status
                response_data = await response.json()
                response_cookies = response.cookies
                
                if response.status == 200:
                    token = response_data.get("token")
                    if token is not None:
                        self.logger.debug(f"Got Authentication Token")
                        self._session.headers.update(
                            {
                                "Authorization": f"Bearer {token}"
                            }
                        )
                        auth_success = True
                        self.logger.info("Authentication success.")
                
                if response.status == 401:
                    self.logger.error(f"Authentication Error. Check username and password. {response_data}")
                    raise AuthenticationFailed()
            
            except AuthenticationFailed as e:
                self.logger.error(f"Authentication Error. Cannot authenticate to {self.base_url}. Exception: {repr(e)}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected Exception occuret while authenticating. {repr(e)}")
                raise
            return auth_success

    async def get_error_codes(self):
        return {}
    
    async def get_err_message(self, err_id):
        err_codes = await self.get_error_codes()
        err_message = err_codes.get(err_id)
        return err_message
