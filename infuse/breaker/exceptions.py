class CircuitBreakerError(Exception):
    """
    When calls to a service fails because the circuit is open, this error is
    raised to allow the caller to handle this type of exception differently.
    """

    pass
