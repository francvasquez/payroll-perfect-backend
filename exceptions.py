class AppError(Exception):
    """Base class for all handled application errors"""

    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ValidationError(AppError):
    """Bad input from the client."""

    def __init__(self, message: str):
        super().__init__(message, status_code=400)


class NotFoundError(AppError):
    """Resource not found."""

    def __init__(self, message: str):
        super().__init__(message, status_code=404)
