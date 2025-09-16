class MondayError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class MondayBackoffError(MondayError):
    """class representing backoff error handling."""
    pass

class MondayBadRequestError(MondayError):
    """class representing 400 status code."""
    pass

class MondayUnauthorizedError(MondayError):
    """class representing 401 status code."""
    pass


class MondayForbiddenError(MondayError):
    """class representing 403 status code."""
    pass

class MondayNotFoundError(MondayError):
    """class representing 404 status code."""
    pass

class MondayConflictError(MondayError):
    """class representing 406 status code."""
    pass

class MondayUnprocessableEntityError(MondayBackoffError):
    """class representing 409 status code."""
    pass

class MondayRateLimitError(MondayBackoffError):
    """class representing 429 status code."""
    def __init__(self, message=None, response=None):
        """Initialize the MondayRateLimitError. Parses the 'retry_in_seconds'  from the response (if present) and sets the
            `retry_after` attribute accordingly.
        """
        self.response = response
        self.retry_after = None

        if response is not None:
            response_json = response.json()
            errors = response_json.get("errors", [])
            if errors:
                extensions = errors[0].get("extensions", {})
                retry_in_seconds = extensions.get("retry_in_seconds")
                if retry_in_seconds is not None:
                    self.retry_after = int(retry_in_seconds)

        base_msg = message or "Rate limit or complexity budget exhausted"
        retry_info = f"(Retry after {self.retry_after} seconds.)" \
            if self.retry_after is not None else "(Retry after unknown delay.)"
        full_message = f"{base_msg} {retry_info}"
        super().__init__(full_message, response=response)

class MondayInternalServerError(MondayBackoffError):
    """class representing 500 status code."""
    pass

class MondayNotImplementedError(MondayBackoffError):
    """class representing 501 status code."""
    pass

class MondayBadGatewayError(MondayBackoffError):
    """class representing 502 status code."""
    pass

class MondayServiceUnavailableError(MondayBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": MondayBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": MondayUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": MondayForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": MondayNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": MondayConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": MondayUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": MondayRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": MondayInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": MondayNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": MondayBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": MondayServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}

