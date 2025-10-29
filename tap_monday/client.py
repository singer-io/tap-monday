from typing import Any, Dict, Mapping, Optional, Tuple
import time

import backoff
import requests
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics

from tap_monday.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    MondayError,
    MondayRateLimitError,
    MondayInternalServerError,
    MondayServiceUnavailableError)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300

def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204] or "errors" in response_json:
        if response_json.get("errors"):
            error = "Exception occured"
            error_extension = "Error Code"
            error_messages = response_json.get("errors", [])
            if error_messages:
                error = error_messages[0].get("message")
                error_extension = error_messages[0].get("extensions", {}).get("code")
            message = "HTTP-error-code: {}, Error: {}, Error Extensions: {}".format(response.status_code, error, error_extension)
        else:
            message = "HTTP-error-code: {}, Error: {}".format(
                response.status_code,
                response_json.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(
                    response.status_code, {}).get("message", "Unknown Error")))
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", MondayError)
        raise exc(message, response) from None

def wait_if_retry_after(details):
    """Backoff handler that checks for a 'retry_after' attribute in the exception
    and sleeps for the specified duration to respect API rate limits.
    """
    exc = details['exception']
    if hasattr(exc, 'retry_after') and exc.retry_after is not None:
        time.sleep(exc.retry_after)  # Force exact wait

class Client:
    """
    A Wrapper class.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://api.monday.com/v2"
        self.api_version = "2025-07"

        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    @property
    def headers(self) -> Dict[str, str]:
        """
        Construct and return the HTTP headers required for the requests.
        """
        header = {
            'Content-Type': 'application/json'
        }
        header['API-Version'] = self.api_version
        return header

    def authenticate(self, headers: Optional[Dict], params: Optional[Dict]) -> Tuple[Dict, Dict]:
        """Provides authenticated headers"""
        result_headers = self.headers.copy()
        result_headers["Authorization"] = f"{self.config['api_token']}"
        if headers:
            result_headers.update(headers)
        return result_headers, params

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None
    ) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        """
        params = params or {}
        headers = headers or {}
        body = body or {}
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request(method, endpoint, headers=headers, params=params, data=body, timeout=self.request_timeout)

    @backoff.on_exception(
        wait_gen=lambda: backoff.expo(factor=2),
        on_backoff=wait_if_retry_after,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            MondayRateLimitError,
            MondayInternalServerError,
            MondayServiceUnavailableError
        ),
        max_tries=5
    )
    def __make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Mapping[Any, Any]]:
        """
        Performs HTTP Operations
        Args:
            method (str): represents the state file for the tap.
            endpoint (str): url of the resource that needs to be fetched
            params (dict): A mapping for url params eg: ?name=Avery&age=3
            headers (dict): A mapping for the headers that need to be sent
            body (dict): only applicable to post request, body of the request

        Returns:
            Dict,List,None: Returns a `Json Parsed` HTTP Response or None if exception
        """
        with metrics.http_request_timer(endpoint) as timer:
            method = method.upper()
            if method not in ("GET", "POST"):
                raise ValueError(f"Unsupported method: {method}")

            if method == "GET":
                kwargs.pop("data", None)
            response = self._session.request(method, endpoint, **kwargs)
            raise_for_error(response)

        return response.json()

