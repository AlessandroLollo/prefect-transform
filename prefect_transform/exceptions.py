"""
Exceptions to be used when interacting with Transform.
"""


class TransformConfigurationException(Exception):
    """
    Exception to raise when a Transform task is misconfigured.
    """

    pass


class TransformRuntimeException(Exception):
    """
    Exception to raise when a Transform task fails to execute.
    """

    pass


class TransformAuthException(Exception):
    """
    Exception to raise in case of auth issues.
    """
