from contextlib import contextmanager
from vistir import spin


def attrs_to_uri(user, passwd, host, port, db):
    """Receives db parameters and converts them into a sqlalchemy resource URI.

    Returns a string preformatted for sqlalchemy.
    """

    if any(v == '' for v in list(locals().values())):
        raise ValueError('All arguments must be present.')
    return "postgresql://{}:{}@{}:{}/{}".format(user, passwd, host, port, db)


@contextmanager
def create_spinner(text):
    """Creates a pretty spinner for our CLI progress indicator"""
    with spin.create_spinner(
            spinner_name="dots",
            start_text=text,
            handler_map={},
            nospin=False,
            write_to_stdout=True
    ) as sp:
        yield sp


def intervals(start, end, n):
    """Yield successive n-sized interval pairs from start to end."""
    if start > end:
        raise ValueError("start must be smaller or equal to end!")
    r = start
    while True:
        r += n
        if r > end + n:
            break
        yield (int(r - n), int(min(r - 1, end)))
