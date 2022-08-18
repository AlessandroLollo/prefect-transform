"""
Exceptions to be used when interacting with Transform.
"""


class TransformRuntimeException(Exception):
    """
    Exception to raise when a Transform task fails to execute.
    """

    pass


class TransformAuthException(Exception):
    """
    Exception to raise in case of auth issues.
    """
