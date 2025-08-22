import pytest
from unittest.mock import patch
from requests import Timeout, ConnectionError
from tap_monday.client import Client, raise_for_error
from tap_monday.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    MondayError
)


class MockResponse:
    """Class to mock response object"""
    def __init__(self, status_code=200, json_data=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}

    def json(self):
        return self._json_data


@pytest.mark.parametrize(
    "status_code, error_json, expected_exception",
    [
        pytest.param(
            429,
            {"errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT"}}]},
            ERROR_CODE_EXCEPTION_MAPPING.get(429).get("raise_exception", MondayError),
            id="Rate Limit 429"
        ),
        pytest.param(
            500,
            {"errors": [{"message": "Internal Server Error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]},
            ERROR_CODE_EXCEPTION_MAPPING.get(500).get("raise_exception", MondayError),
            id="Internal Server Error 500"
        ),
        pytest.param(
            401,
            {"errors": [{"message": "Internal Server Error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]},
            ERROR_CODE_EXCEPTION_MAPPING.get(401).get("raise_exception", MondayError),
            id="Unauthorized 401"
        )
    ],
)
def test_raise_for_error(status_code, error_json, expected_exception):
    """Raise execption on the bases of status code"""
    response = MockResponse(status_code=status_code, json_data=error_json)
    with pytest.raises(expected_exception):
        raise_for_error(response)


@pytest.mark.parametrize(
    "side_effects, expected_result, expected_exception",
    [
        pytest.param(
            [MockResponse(200, {"data": {"result": "ok"}})],
            {"data": {"result": "ok"}},
            None,
            id="Successful request"
        ),
        pytest.param(
            [
                MockResponse(429, {"errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT", "retry_in_seconds": 1}}]}),
                MockResponse(200, {"data": {"result": "ok"}}),
            ],
            {"data": {"result": "ok"}},
            None,
            id="Retry after rate limit then success"
        ),
        pytest.param(
            [MockResponse(429, {"errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT", "retry_in_seconds": 1}}]})] * 5,
            None,
            ERROR_CODE_EXCEPTION_MAPPING.get(429).get("raise_exception", MondayError),
            id="Repeated rate limit errors"
        ),
        pytest.param(
            [MockResponse(401, {"errors": [{"message": "Unauthorized Error", "extensions": {"code": "Unauthorized Request"}}]})],
            None,
            ERROR_CODE_EXCEPTION_MAPPING.get(401).get("raise_exception", MondayError),
            id="Unauthorized error"
        ),
        pytest.param(
            [Timeout("Request timed out")] * 5,
            None,
            Timeout,
            id="Repeated timeouts"
        ),
        pytest.param(
            [ConnectionError("Connection failed")] * 5,
            None,
            ConnectionError,
            id="Repeated connection errors"
        ),
        pytest.param(
            [MockResponse(500, {"errors": [{"message": "Internal Server Error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]})] * 5,
            None,
            ERROR_CODE_EXCEPTION_MAPPING.get(500).get("raise_exception", MondayError),
            id="Repeated internal server errors"
        )
    ]
)
def test_client_make_request(side_effects, expected_result, expected_exception):
    """Test the client's request-making behavior under various simulated conditions."""

    config = {
        "api_token": "dummy_token",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-monday test@test.com",
        "api_version": "2025-07",
        "request_timeout": 300
        }
    client = Client(config)

    with patch("requests.Session.request", side_effect=side_effects) as mock_request, \
         patch("backoff.expo", return_value=(0 for _ in range(10))) as mock_expo, \
         patch("time.sleep", return_value=None) as mock_sleep:
        if expected_exception:
            with pytest.raises(expected_exception):
                client.make_request("POST", "/dummy", body={"query": "query { test }"})
        else:
            result = client.make_request("POST", "/dummy", body={"query": "query { test }"})
            assert result == expected_result

        assert mock_request.call_count == len(side_effects)

