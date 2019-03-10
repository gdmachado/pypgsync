import re
import sqlalchemy

from contextlib import contextmanager
from sqlalchemy import asc, select
from sqlalchemy.sql import func, text
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_


from sqlalchemy_utils import database_exists


from pypgsync.utils import attrs_to_uri, intervals


class Session(object):
    """Session class, mainly used for interacting with two dbs at the same
    time.
    """

    def __init__(self, host, port, src_db, dst_db, tbl, user, passwd, chunksize, max_updated):
        """Constructor for the Session class. Basically instantiate sqlalchemy
        engines for both sourceDB and destinationDB, fetch table metadata, and
        create destination table if necessary.
        """

        self.src_engine = self._engine(attrs_to_uri(
            user, passwd, host, port, src_db))
        self.dst_engine = self._engine(attrs_to_uri(
            user, passwd, host, port, dst_db))
        self.table_name = tbl
        self.chunksize = chunksize
        self.max_updated = max_updated

        self._init_db()

        self.src_table = Table(self.table_name, self.src_metadata,
                               autoload=True)
        self.dst_table = Table(self.table_name, self.dst_metadata,
                               autoload=True)

        self.starting_point = self._get_starting_point()
        self.slices, self.src_table_size = self.calculate_optimal_slices()

        super(Session, self).__init__()

    def _get_starting_point(self):
        """This method basically gets the MAX(updated) from the destination
        table. This will serve as a starting point for us, in the case we are
        not on our first run of the sync operation, and some rows have already
        been migrated to the destination table.
        """

        with self.connect() as (src, dst):
            stmt = select([func.max(self.dst_table.c.updated)])
            min = dst.execute(stmt).scalar()

            if not min:
                stmt = select([func.min(self.src_table.c.updated)])
                min = src.execute(stmt).scalar()

            return min

    @classmethod
    def _chunkify(self, conn, statement, column, chunksize):
        """This creates a generator yielding data chunks of size chunkSize.
        Expects result to be a sqlalchemy ResultProxy object.
        """

        for window, len in self.windowed_query(conn, statement, column, chunksize):
            try:
                result = conn.execute(window)

                yield result.fetchall(), len
            finally:
                # close resultset instead of relying on garbage collection
                # in the event of an exception
                result.close()

    @classmethod
    def _engine(self, db_uri):
        # use batch mode for more efficient batch inserts
        return sqlalchemy.create_engine(db_uri, use_batch_mode=True)

    @contextmanager
    def connect(self):
        """Procures a connection from one of the sqlalchemy engines for
        source or destination dbs.

        This is decorated with @contextmanager so we can gracefully spawn
        and automatically close connections via `with` statements like so:

        with self.connect() as (src, dst):
            # database stuff on src
            # database stuff on dst

        Returns a tuple of (source_connection, destination_connection)
        """

        conn = self.src_engine.connect(), self.dst_engine.connect()

        try:
            yield conn
        finally:
            # close both connections
            (c.close() for c in conn)

    def merge_chunks(self):
        """For each slice in self.slices, spawn two connections to source and
        destination DBs, build the upsert statement, and then for each chunk,
        execute the upsert statement, yielding current progress and total
        number of rows to be processed.
        """

        # progress counter
        processed_rowcount = 0

        for slice in self.slices:
            # spawns connection via contextmanager so it gracefully closes once
            # we're done
            with self.connect() as (src, dst):
                # build the select statement
                # this order-by makes sure we will upsert in the correct order
                select_stmt = \
                    select([self.src_table]) \
                    .where(
                        and_(self.src_table.c.updated >= slice[0],
                             self.src_table.c.updated <= slice[1])
                    ) \
                    .order_by(asc(self.src_table.c.updated))

                insert_stmt = insert(self.dst_table)

                # dict with non-primary key columns
                update_cols = {
                    c.name: c for c in insert_stmt.excluded if not c.primary_key}

                # upsert statement on primary key conflicts
                insert_stmt = insert_stmt.on_conflict_do_update(
                    constraint=self.dst_table.primary_key,
                    set_=update_cols)

                # walk the resultset and perform inserts incrementally, in chunks
                for chunk, rowcount in self._chunkify(src, select_stmt, self.src_table.c.updated, self.chunksize):
                    # execute the upsert statement as execute_batch
                    dst.execute(
                        insert_stmt, [dict(zip(self.src_table.columns.keys(), r))
                                      for r in chunk])

                    # increment our progress counter
                    processed_rowcount += len(chunk)

                    # yield current progress and total.
                    # when using psycopg2 in batch mode, we can't know the
                    # executed statement's rowcount, so make sure we don't
                    # increment over the total number of rows.
                    yield min(processed_rowcount, rowcount), rowcount, self.src_table_size

    def _init_db(self):
        """Checks if both databases and tables exists. If destination table
        doesn't exist, creates it.

        Raises RuntimeError if source table or either of the two databases
        don't exist.
        """

        try:
            if not database_exists(self.src_engine.url) or not \
                    database_exists(self.dst_engine.url):
                raise RuntimeError(
                    'Source or destination db does not exist ¯\_(ツ)_/¯')
        except sqlalchemy.exc.OperationalError as e:
            if "password" in e.args[0]:
                raise RuntimeError(
                    'Unable to log in with that user and password (╯°□°）╯︵ ┻━━━━┻')
            if "could not connect" in e.args[0] or "starting up" in e.args[0]:
                raise RuntimeError(
                    'Database unreachable ¯\_(⊙︿⊙)_/¯')

        # get metadata for both dbs
        self.src_metadata = MetaData(self.src_engine, reflect=True)
        self.dst_metadata = MetaData(self.dst_engine, reflect=True)

        if self.table_name not in self.src_metadata.tables:
            import pdb
            pdb.set_trace()
            raise RuntimeError(
                'Table "{}" does not exist in source db ¯\_(ツ)_/¯'
                .format(self.table_name))

        if self.table_name not in self.dst_metadata.tables:
            # create table in destination db if it doesn't exist
            tbl = Table(self.table_name, self.src_metadata, autoload=True)
            tbl.metadata.create_all(self.dst_engine)

        return self

    def calculate_optimal_slices(self):
        """Split the source table into approximately equally-sized slices. This
        assumes we are splicing based on the `updated` column, and ideally it
        would be equally distributed - the lest homogeneous data is distributed
        across `updated`, the less equally-sized all slices will be.

        Makes sure we are always working on slices of at most 10 million rows (
        currently hard-coded).
        """

        with self.connect() as (src, dst):
            # get source table's max updated value
            src_max = src.execute(
                select([func.max(self.src_table.c.updated)])).scalar()

            if self.starting_point >= src_max:
                # no work to do
                return [], 0
            # get fast approximation for result size with EXPLAIN
            resultsize_stmt = select([self.src_table]) \
                .where(self.src_table.c.updated >= self.starting_point) \
                .where(self.src_table.c.updated <= self.max_updated)

            # manually craft an EXPLAIN query
            resultsize_query = 'EXPLAIN ' + \
                str(
                    resultsize_stmt.compile(
                        dialect=postgresql.dialect(),
                        compile_kwargs={"literal_binds": True}))

            result = src.execute(resultsize_query).scalar()

            # extract rowcount from explain result
            m = int(re.search('rows=([0-9]*)', result).groups()[0])

            # ideal length for each interval so that eaach slice has 10m rows
            slice_length = (self.max_updated
                            - self.starting_point) / m * 10_000_000

            return list(intervals(self.starting_point, self.max_updated, slice_length)), m

    @classmethod
    def windowed_query(self, conn, select_stmt, column, windowsize):
        """Generator that yields filter ranges against a given column that break
        it into windows.

        Initially executes something like this:
            SELECT column
            FROM
                (SELECT column, ROW_NUMBER() OVER (ORDER BY column) as rownum
                 FROM column.Table) as subquery
            WHERE subquery.rownum % windowsize = 1;

        With the result from that, yield select statements using the values
        returned against select_stmt.

        It's important to do this so we don't force the server to load the entire
        resultset into memory before we even start fetching chunks.

        Adapted from https://github.com/sqlalchemy/sqlalchemy/wiki/WindowedRangeQuery

        """

        def interval_to_expr(start, end):
            if end:
                return and_(
                    column >= start,
                    column < end
                )
            else:
                return column >= start

        stmt = \
            select([
                column,
                func.row_number()
                    .over(order_by=column)
                    .label('rownum')
            ])

        # apply where clauses from original select_stmt
        stmt._whereclause = select_stmt._whereclause

        # turn this into a subquery
        stmt = stmt.alias('anon_1')

        # select from subquery
        q = select([getattr(stmt.c, column.name)])

        if windowsize > 1:
            # apply modulo windowsize to determine all starting points
            q = q.where(sqlalchemy.text("rownum %% %d=1" % windowsize))

        # execute query
        windows = [n for n in conn.execute(q).fetchall()]

        # this rowcount will be innacurate with very large window sizes
        rowcount = len(windows) * windowsize

        while windows:
            # yield all windows in ascending order
            start = windows.pop(0)[0]
            if windows:
                end = windows[0][0]
            else:
                end = None
            # feed back the approximate total result size so we can calculate progress
            yield select_stmt.where(interval_to_expr(start, end)), rowcount
