"""
for Python 3
"""


def raise_from(exccls, message, exc):
    """
    raise exc with new message
    """
    raise exccls(message) from exc
