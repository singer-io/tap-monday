from typing import Any, Dict, Mapping, Optional, Tuple
import json

import backoff
import requests
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError

from singer import get_logger, metrics

from tap_monday.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    MondayError,
    MondayCursorExpiredError,
    MondayForbiddenError,
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
            error_messages = response_json.get("errors", [])
            # Scan *all* errors; CursorException may not be the first entry.
            # Use a default of None so the absence of CursorException is handled
            # gracefully instead of raising StopIteration.
            cursor_error = next(
                (e for e in error_messages
                 if e.get("extensions", {}).get("code") == "CursorException"),
                None,
            )
            if cursor_error:
                message = "HTTP-error-code: {}, Error: {}, Error Extensions: {}".format(
                    response.status_code,
                    cursor_error.get("message"),
                    "CursorException",
                )
                raise MondayCursorExpiredError(message, response) from None
            # Non-cursor GraphQL error — fall through to generic handling.
            error = error_messages[0].get("message", "Exception occurred")
            error_extension = error_messages[0].get("extensions", {}).get("code", "Error Code")
            message = "HTTP-error-code: {}, Error: {}, Error Extensions: {}".format(
                response.status_code, error, error_extension)
        else:
            message = "HTTP-error-code: {}, Error: {}".format(
                response.status_code,
                response_json.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(
                    response.status_code, {}).get("message", "Unknown Error")))
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", MondayError)
        # Monday returns HTTP 200 for GraphQL-level errors. Map known extension
        # codes to the appropriate exception so callers can handle them.
        if response.status_code == 200 and response_json.get("errors"):
            error_code = response_json["errors"][0].get("extensions", {}).get("code", "")
            _GRAPHQL_ERROR_CODE_MAPPING = {
                "UserUnauthorizedException": MondayForbiddenError,
                "INTERNAL_SERVER_ERROR": MondayInternalServerError,
            }
            exc = _GRAPHQL_ERROR_CODE_MAPPING.get(error_code, exc)
        raise exc(message, response) from None

def get_retry_after(exception_info):
    """Returns the retry_after value from RateLimitError exception.
    This is used by backoff.runtime to determine wait time.
    """
    exception = exception_info.get('exception') if isinstance(exception_info, dict) else exception_info

    if exception and isinstance(exception, MondayRateLimitError):
        retry_after = exception.retry_after or 60
        LOGGER.info(f"Rate limited. Waiting {retry_after} seconds...")
        return retry_after

    return 60  # Default fallback

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

    def check_access(self):
        """Validate the API token by making a lightweight authenticated request.

        Raises MondayUnauthorizedError (401) or MondayForbiddenError (403) if
        the token is invalid or lacks required permissions.
        """
        body = json.dumps({"query": "query { me { id } }"})
        self.make_request("POST", self.base_url, body=body)

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

    def probe_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Single-shot request with no backoff or retry — intended for access
        probes during discovery where retrying is not appropriate."""
        params = params or {}
        headers = headers or {}
        body = body or {}
        headers, params = self.authenticate(headers, params)
        response = self._session.request(
            method.upper(), endpoint,
            headers=headers, params=params, data=body,
            timeout=self.request_timeout,
        )
        raise_for_error(response)
        return response.json()

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            MondayInternalServerError,
            MondayServiceUnavailableError,
        ),
        max_tries=5,
        factor=2,
        giveup=lambda e: isinstance(e, MondayRateLimitError),
    )
    @backoff.on_exception(
        backoff.runtime,
        exception=(
            MondayRateLimitError,
        ),
        max_tries=5,
        value=get_retry_after,
        jitter=None,
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

