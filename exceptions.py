class AppError(Exception):
    """Base class for all handled application errors"""

    def __init__(
        self,
        message: str,
        status_code: int = 400,
        error_code: str | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        super().__init__(message)


class ValidationError(AppError):
    """Bad input from the client."""

    def __init__(self, message: str):
        super().__init__(message, status_code=400)


class NotFoundError(AppError):
    """Resource not found."""

    def __init__(self, message: str):
        super().__init__(message, status_code=404)


# Client-facing error codes (returned in API body as `code`)
WFN_SYSTEM_UNRECOGNIZED = "WFN_SYSTEM_UNRECOGNIZED"
TA_SYSTEM_UNRECOGNIZED = "TA_SYSTEM_UNRECOGNIZED"

WFN_SYSTEM_UNRECOGNIZED_MESSAGE = (
    "Could not determine WFN system type. Please check WFN file contents "
    "to ensure they match the correct format."
)
TA_SYSTEM_UNRECOGNIZED_MESSAGE = (
    "Could not determine Time & Attendance system type. Please check your "
    "time card file contents to ensure they match the correct format."
)
