import unittest
from parameterized import parameterized
from unittest.mock import patch, MagicMock
from requests import Timeout, ConnectionError

from tap_monday.client import Client, raise_for_error, get_retry_after
from tap_monday.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    MondayError,
    MondayRateLimitError,
    MondayUnauthorizedError
)


class MockResponse:
    """Class to mock response object"""
    def __init__(self, status_code=200, json_data=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}

    def json(self):
        return self._json_data


class TestRaiseForError(unittest.TestCase):
    @parameterized.expand([
        (
            "Rate Limit 429",
            429,
            {"errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT"}}]},
            ERROR_CODE_EXCEPTION_MAPPING.get(429).get("raise_exception", MondayError)
        ),
        (
            "Internal Server Error 500",
            500,
            {"errors": [{"message": "Internal Server Error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]},
            ERROR_CODE_EXCEPTION_MAPPING.get(500).get("raise_exception", MondayError)
        ),
        (
            "Unauthorized 401",
            401,
            {"errors": [{"message": "Internal Server Error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]},
            ERROR_CODE_EXCEPTION_MAPPING.get(401).get("raise_exception", MondayError)
        ),
    ])
    def test_raise_for_error(self, name, status_code, error_json, expected_exception):
        """Test raise_for_error raises the expected exception based on status code."""
        response = MockResponse(status_code=status_code, json_data=error_json)
        with self.assertRaises(expected_exception):
            raise_for_error(response)


class TestClientMakeRequest(unittest.TestCase):

    def setUp(self):
        self.config = {
            "api_token": "dummy_token",
            "start_date": "2019-01-01T00:00:00Z",
            "user_agent": "tap-monday test@test.com",
            "api_version": "2025-07",
            "request_timeout": 300
        }
        self.client = Client(self.config)

    @parameterized.expand([
        (
            "Successful request",
            [MockResponse(200, {"data": {"result": "ok"}})],
            {"data": {"result": "ok"}},
            None
        ),
        (
            "Retry after rate limit then success",
            [
                MockResponse(429, {
                    "errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT", "retry_in_seconds": 1}}]
                }),
                MockResponse(200, {"data": {"result": "ok"}})
            ],
            {"data": {"result": "ok"}},
            None
        ),
        (
            "Repeated rate limit errors",
            [MockResponse(429, {
                "errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT", "retry_in_seconds": 1}}]
            })] * 5,
            None,
            ERROR_CODE_EXCEPTION_MAPPING.get(429).get("raise_exception", MondayError)
        ),
        (
            "Unauthorized error",
            [MockResponse(401, {
                "errors": [{"message": "Unauthorized Error", "extensions": {"code": "Unauthorized Request"}}]
            })],
            None,
            ERROR_CODE_EXCEPTION_MAPPING.get(401).get("raise_exception", MondayError)
        ),
        (
            "Repeated timeouts",
            [Timeout("Request timed out")] * 5,
            None,
            Timeout
        ),
        (
            "Repeated connection errors",
            [ConnectionError("Connection failed")] * 5,
            None,
            ConnectionError
        ),
        (
            "Repeated internal server errors",
            [MockResponse(500, {
                "errors": [{"message": "Internal Server Error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]
            })] * 5,
            None,
            ERROR_CODE_EXCEPTION_MAPPING.get(500).get("raise_exception", MondayError)
        ),
    ])
    def test_client_make_request(self, name, side_effects, expected_result, expected_exception):
        """Test the client's request-making behavior under various simulated conditions."""
        with patch("requests.Session.request", side_effect=side_effects) as mock_request, \
             patch("backoff.expo", return_value=(0 for _ in range(10))), \
             patch("time.sleep", return_value=None):

            if expected_exception:
                with self.assertRaises(expected_exception):
                    self.client.make_request("POST", "/dummy", body={"query": "query { test }"})
            else:
                result = self.client.make_request("POST", "/dummy", body={"query": "query { test }"})
                self.assertEqual(result, expected_result)

            self.assertEqual(mock_request.call_count, len(side_effects))


class TestCheckAccess(unittest.TestCase):
    """Test the Client.check_access() token validation method."""

    def setUp(self):
        self.config = {
            "api_token": "dummy_token",
            "start_date": "2019-01-01T00:00:00Z",
            "user_agent": "tap-monday test@test.com",
            "api_version": "2025-07",
            "request_timeout": 300
        }
        self.client = Client(self.config)

    def test_check_access_valid_token(self):
        """check_access succeeds without error when the token is valid."""
        mock_response = MockResponse(200, {"data": {"me": {"id": "12345"}}})
        with patch("requests.Session.request", return_value=mock_response) as mock_request:
            self.client.check_access()
            mock_request.assert_called_once()
            # Verify it sent the me query
            call_kwargs = mock_request.call_args
            self.assertIn("me", call_kwargs.kwargs.get("data", "") or call_kwargs[1].get("data", ""))

    def test_check_access_invalid_token(self):
        """check_access raises MondayUnauthorizedError when the token is invalid."""
        mock_response = MockResponse(
            401,
            {"errors": [{"message": "Not Authenticated", "extensions": {"code": "Unauthorized"}}]})
        with patch("requests.Session.request", return_value=mock_response):
            with self.assertRaises(MondayUnauthorizedError):
                self.client.check_access()


class TestGetRetryAfter(unittest.TestCase):
    """Test the get_retry_after backoff value function."""

    def test_rate_limit_error_with_retry_after(self):
        """Returns retry_after seconds from a MondayRateLimitError."""
        mock_response = MockResponse(
            status_code=429,
            json_data={"errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT", "retry_in_seconds": 15}}]}
        )
        exc = MondayRateLimitError(response=mock_response)
        self.assertEqual(get_retry_after({'exception': exc}), 15)

    def test_rate_limit_error_with_none_retry_after(self):
        """Returns 60 (default) when retry_after is None on a MondayRateLimitError."""
        mock_response = MockResponse(status_code=429, json_data={"errors": [{"message": "Rate limit"}]})
        exc = MondayRateLimitError(response=mock_response)
        self.assertEqual(get_retry_after({'exception': exc}), 60)

    def test_non_rate_limit_exception_returns_default(self):
        """Returns 60 when the exception is not a MondayRateLimitError."""
        self.assertEqual(get_retry_after({'exception': Exception("other")}), 60)

    def test_no_exception_returns_default(self):
        """Returns 60 when no exception key is present."""
        self.assertEqual(get_retry_after({}), 60)

    def test_exception_passed_directly(self):
        """Accepts the exception object directly (not wrapped in a dict)."""
        mock_response = MockResponse(
            status_code=429,
            json_data={"errors": [{"message": "Rate limit", "extensions": {"code": "RATE_LIMIT", "retry_in_seconds": 30}}]}
        )
        exc = MondayRateLimitError(response=mock_response)
        self.assertEqual(get_retry_after(exc), 30)

