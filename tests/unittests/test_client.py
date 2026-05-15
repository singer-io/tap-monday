import unittest
from parameterized import parameterized
from unittest.mock import patch, MagicMock
from requests import Timeout, ConnectionError

from tap_monday.client import Client, raise_for_error, wait_if_retry_after
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


class TestWaitIfRetryAfter(unittest.TestCase):
    """Test the wait_if_retry_after backoff handler function."""

    @patch('time.sleep')
    def test_wait_if_retry_after_with_retry_after_attribute(self, mock_sleep):
        """Test that the handler sleeps for the exact duration from retry_after."""
        # Create a mock exception with retry_after attribute
        mock_exception = MagicMock()
        mock_exception.retry_after = 5

        details = {'exception': mock_exception}
        wait_if_retry_after(details)

        # Verify it slept for the exact duration
        mock_sleep.assert_called_once_with(5)

    @patch('time.sleep')
    def test_wait_if_retry_after_with_none_retry_after(self, mock_sleep):
        """Test that the handler does not sleep when retry_after is None."""
        mock_exception = MagicMock()
        mock_exception.retry_after = None

        details = {'exception': mock_exception}
        wait_if_retry_after(details)

        # Verify it did not sleep
        mock_sleep.assert_not_called()

    @patch('time.sleep')
    def test_wait_if_retry_after_without_retry_after_attribute(self, mock_sleep):
        """Test that the handler does not sleep when exception has no retry_after attribute."""
        mock_exception = MagicMock(spec=[])

        details = {'exception': mock_exception}
        wait_if_retry_after(details)

        # Verify it did not sleep
        mock_sleep.assert_not_called()

    @patch('time.sleep')
    def test_wait_if_retry_after_with_exception_in_args(self, mock_sleep):
        """Test that the handler finds the exception in args[0] when not in 'exception' key."""
        mock_exception = MagicMock()
        mock_exception.retry_after = 10

        details = {'args': (mock_exception,)}
        wait_if_retry_after(details)

        # Verify it slept for the exact duration
        mock_sleep.assert_called_once_with(10)

    @patch('time.sleep')
    def test_wait_if_retry_after_with_no_exception(self, mock_sleep):
        """Test that the handler does not sleep when no exception is present."""
        details = {}
        wait_if_retry_after(details)

        # Verify it did not sleep
        mock_sleep.assert_not_called()

    @patch('time.sleep')
    def test_wait_if_retry_after_with_real_rate_limit_error(self, mock_sleep):
        """Test with an actual MondayRateLimitError instance."""
        # Create a mock response with retry_in_seconds
        mock_response = MockResponse(
            status_code=429,
            json_data={
                "errors": [{
                    "message": "Rate limit exceeded",
                    "extensions": {
                        "code": "RATE_LIMIT",
                        "retry_in_seconds": 15
                    }
                }]
            }
        )

        # Create actual exception with the response
        exc = MondayRateLimitError(response=mock_response)

        details = {'exception': exc}
        wait_if_retry_after(details)

        # Verify it slept for the exact duration from the API response
        mock_sleep.assert_called_once_with(15)

    @patch('time.sleep')
    def test_wait_if_retry_after_with_empty_args(self, mock_sleep):
        """Test that the handler handles empty args gracefully."""
        details = {'args': ()}
        wait_if_retry_after(details)

        # Verify it did not sleep
        mock_sleep.assert_not_called()

