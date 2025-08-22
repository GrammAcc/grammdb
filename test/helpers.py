from contextlib import contextmanager

import pytest


@contextmanager
def does_not_raise(exception=Exception):
    """The opposite of `pytest.raises`."""

    try:
        yield
    except exception as e:
        raise pytest.fail("Raised {0}".format(type(e)))
