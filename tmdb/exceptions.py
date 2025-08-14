class RetryableError(Exception):
    """Exception raised when request can be retried."""

    def __init__(self, *args, status: int = None):
        super().__init__(*args)
        self.status = status
