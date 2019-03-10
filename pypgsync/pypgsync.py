import time

from pypgsync.session import Session


def start_single(host, port, src_db, dst_db, tbl, user, passwd, chunksize):
    """API wrapper for Session in the form of a generator - purely spawns a
    Session object, calls Session.merge_chunks() and yields each single
    processed chunk in the form of rows processed and total rows.
    """

    # execution time used for limiting current fetch
    max_updated = int(time.time() * 1000)

    session = Session(
        host=host,
        port=port,
        src_db=src_db,
        dst_db=dst_db,
        tbl=tbl,
        user=user,
        passwd=passwd,
        chunksize=chunksize,
        max_updated=max_updated)

    for r in session.merge_chunks():
        yield r
