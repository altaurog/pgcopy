"""
for Python 2
"""
import sys

def raise_from(exccls, message, exc):
    """
    raise exc with new message
    """
    template = '{}\nCause: {}: {}'
    newexc = exccls(template.format(
        message, exc.__class__.__name__, str(exc),
    ))
    raise newexc, None, sys.exc_info()[2]
