
# $ pypgsync

This application has been developed as a solution for the challenge sent as part of the Senior Data Engineer position hiring process at N26 (https://n26.com/en/careers/positions/1519951).

## Usage

```shell
# install using Pyhton 3.7.2
$ pip install .

# run test suite
$ pip install pytest && pytest

$ pypgsync --help
Usage: pypgsync [OPTIONS] COMMAND [ARGS]...

  This script syncs data from a source PostgreSQL table into a destination
  table with the same structure. Both tables must be in the same PostgreSQL
  server. If the destination table does not exist, the script will create
  it. It also expects both tables to share the same name.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  continuous  Run in continous mode
  single      Start sync in single-time mode.

$ pypgsync continous --help
Usage: pypgsync continuous [OPTIONS] SOURCE_DB DESTINATION_DB TABLENAME
                           USERNAME

  Continuous mode basically executes the same algorithm for single-time
  mode, but continuously repeating in order to keep the two tables in sync,
  waiting `delay` seconds between each run.

Options:
  -h, --hostname TEXT      Hostname  [default: localhost; required]
  -p, --port INTEGER       Port  [default: 5432; required]
  -c, --chunksize INTEGER  Transaction chunk size  [default: 1000; required]
  -d, --delay INTEGER      Time in seconds to wait between executions
                           [default: 5]
  --password TEXT
  --help                   Show this message and exit.

# start continuous mode
$ pypgsync continuous \
--hostname challenge-tz1-fra-rds.cfjzrfcbcmn1.eu-central-1.rds.amazonaws.com \
--port 2626 \
--chunksize 10000 n26_data_case rome transactions deadpool
```

## Challenge

Implement a script/algorithm that can ***incrementally*** and ***efficiently*** extract all data from a source table into a destination table.

## Assumptions

- PostgreSQL version: PostgreSQL 9.6.9 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.3 20140911 (Red Hat 4.8.3-9), 64-bit
- Both tables live in the same PostgreSQL server, but in different databases
- Destination database has been already created
- Only read-access is possible on the source table
- Both tables will have the same structure
- Everything is in the `public` schema of respective databases

## Solutions

### Question 1 - Challenge

#### Files

The main files to be considered are:

1. `./pypgsync/session.py`
  This is where most of the database logic is implemented.
2. `./pypgsync/utils.py`
  This provides some utilities and functionality
3. `./pypgsync/pypgsync.py`
  This is purely wrapping the Session API - this is called by the CLI code, and calls Session code
4. `./pypgsync/cli.py`
  This is only implementing logic for the CLI functionality
5. `./docker-compose.yml`
  Docker compose file used to test postgres locally - not super relevant but for reference to performance results

The code can be checked in this repository, but as an overview, the idea behind the algorithm
is:

1. Assign the current time to an upper time limit for the algorithm, and assign `MAX(updated) FROM destination_table` to the lower time limit. If the destination table is empty, this will be equal to `MIN(updated) FROM source_table`. Creates destination table if it doesn't exist. This ensures we can always stop and start the program again without issues.
2. Using the time limits from the previous step, run an `EXPLAIN` query on the source table to determine the approximate row count (refer to `session.calculate_optimal_slices` and `utils.intervals`). Using this, calculate slices or intervals that would have approximately 10 million rows each. This is to further optimize reading data from the source table - I've arbitrarily chose 10 million, but this could probably be fine-tuned further.
3. Perform a windowed range query (refer to `session.windowed_query` and `session._chunkify`), to determine _windows_ of the table that we will want to query upon. This is mainly performed to avoid a full `SELECT * FROM table` to unnecessarily load data onto the server's memory. This is performed through something like this:
    ```sql
    SELECT updated FROM (
        SELECT updated,
               ROW_NUMBER() OVER (ORDER BY updated DESC) as rownum
               WHERE ...
               -- apply defined lower and upper time limits here
    ) t
    WHERE rownum % WINDOW_SIZE = 1;
    ```
    This query will divide our table into equally-sized windows, returning us the starting points as values for `updated`.
4. For each `slice` and `chunk` (or window), simultaneously traverse the chunks (refer to `session.merge_chunks`), in ascending `updated` order, via two connections, one to the source database and one to the destination database, first `SELECT`ing rows from the source table, and with the result from that, performing an `UPSERT` (or `INSERT ON CONFLICT UPDATE`) statement. The `UPSERT` is also performed via ascending `updated` order, to make sure we insert/update data in the same order as the source table. The `SELECT` statement is "chunkified" according to the `chunksize` parameter passed to the command-line interface (default: 10000). We also make use of the `execute_batch` functionality from the `psycopg2` library to insert data more efficiently.
5. Once we exhaust all slices, end the algorithm
6. (optional) when running the command-line application in the `continuous` mode, steps 1 through 5 will repeat until interrupted.

#### Conclusion and Considerations

- Since the source table would be a production transactional table used by the application, unless there is a very specific reason to do this, cloning such a big table (700m+ records) would probably be better served via replication.
- This could be done more efficiently via an initial load using COPY before running this application.
- This will likely perform much better if ran locally on the server or within the same network as to avoid latency issues.
- There are probably some further fine-tunings that could be done, but with the limited time window I wasn't able to. I took most steps to keep performance and resource impact to the source database to a minimum.
- Since this is a database-focused application, ideally I would also implement integration tests via a CI solution, but due to the limit time frame I haven't.

With this I was able to get a performance of about 6000 rows/s running locally on my laptop (2.9 GHz Intel Core i7, 16GB DDR3, postgres running inside a docker container). When connecting to N26's remote database, this went down to about 1200 rows/s.

### Question 2

Additionally to creating the table, I would first create an index on `user_id`
in the new `transactions` table to help with the initial compute of balance per
user, like so:

```sql
create index user_id_idx
  on transactions (user_id);
```

Now, for creating the table:

```sql
create table user_balance
(
  user_id uuid           not null
    constraint user_balance_pkey
      primary key,
  balance numeric(18, 2) not null,
  updated bigint         not null
);

create index user_balance_updated_idx
  on user_balance (updated);
```

Executing the initial compute:

```sql
insert into user_balance (user_id, balance, updated)
select user_id, sum(amount), extract(epoch from now()) * 1000
from transactions
where certified_by_user is not null and status <> 'BLOCKED'
group by 1;
```

Defining trigger function:

```sql
CREATE OR REPLACE FUNCTION user_balance_trigger()
  RETURNS trigger AS
$BODY$
BEGIN
  INSERT INTO user_balance as t (user_id, balance, updated) VALUES
      (NEW.user_id, NEW.amount, extract(epoch from NOW()) * 1000)
  ON CONFLICT (user_id)
    DO UPDATE
      SET balance = t.balance + NEW.amount,
      updated = extract(epoch from NOW()) * 1000
      WHERE t.user_id = NEW.user_id;
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
```

Declaring insert trigger:

```sql
-- Executes when inserts happen on transactions, and transaction is
-- both approved and certified by user.
CREATE TRIGGER user_balance_insert_trigger
  AFTER INSERT ON transactions
  FOR EACH ROW
  WHEN (NEW.certified_by_user IS NOT NULL AND NEW.status <> 'BLOCKED')
  EXECUTE PROCEDURE user_balance_trigger();
```

Declaring update trigger:
```sql
-- Executes for updates on transactions, only when the previous
-- transaction state was either not certified or blocked, and
-- only when the new state is both certified and not blocked.
CREATE TRIGGER user_balance_update_trigger
  AFTER UPDATE OF certified_by_user, status
  ON transactions
  FOR EACH ROW
  WHEN ((OLD.certified_by_user IS NULL OR OLD.status = 'BLOCKED') AND
  NEW.certified_by_user IS NOT NULL AND NEW.status <> 'BLOCKED')
  EXECUTE PROCEDURE user_balance_trigger();
```

### Question 3

#### Item a

Here we can use a partial index on `status = 'BLOCKED'`, albeit with a very marginal
performance gain. If there is need for similar queries but with a different value for
`status`, it's obviously better to go with a non-partial index on status.

Index:

```sql
create index blocked_status_idx
  on transactions (status)
  where status = 'BLOCKED';
```

Test query:
```sql
SELECT COUNT(*) FROM transactions
WHERE status = 'BLOCKED';
```

Explain before:
```sql
Aggregate  (cost=933678.34..933678.35 rows=1 width=8)
  ->  Seq Scan on transactions  (cost=0.00..929226.86 rows=1780590 width=0)
        Filter: (status = 'BLOCKED'::text)
```

Explain after:
```sql
Aggregate  (cost=517832.46..517832.47 rows=1 width=8)
  ->  Bitmap Heap Scan on transactions  (cost=28834.67..513381.00 rows=1780586 width=0)
        Recheck Cond: (status = 'BLOCKED'::text)
        ->  Bitmap Index Scan on blocked_status_idx  (cost=0.00..28389.53 rows=1780586 width=0)
```

#### Item b

Here it doesn't matter if we create a sorted index or a regular one, since indexes
can be scanned in any direction, and the order is already available either way.

Indexes:

```sql
create index user_id_certified_by_user_idx
  on transactions (user_id, certified_by_user);
```

Test query:
```sql
select * from transactions
where certified_by_user is not null
and user_id = 'd4726bab-3d3c-4d2d-8039-563859221271'
order by certified_by_user desc
limit 10;
```

Explain before:
```sql
Limit  (cost=10000929257.66..10000929257.68 rows=10 width=70)
  ->  Sort  (cost=10000929257.66..10000929261.34 rows=1472 width=70)
        Sort Key: certified_by_user DESC
        ->  Seq Scan on transactions  (cost=10000000000.00..10000929225.85 rows=1472 width=70)
              Filter: ((certified_by_user IS NOT NULL) AND (user_id = 'd4726bab-3d3c-4d2d-8039-563859221271'::uuid))
```

Explain after:
```sql
Limit  (cost=0.56..40.93 rows=10 width=70)
  ->  Index Scan Backward using user_id_certified_by_user_idx on transactions  (cost=0.56..5941.69 rows=1472 width=70)
        Index Cond: ((user_id = 'd4726bab-3d3c-4d2d-8039-563859221271'::uuid) AND (certified_by_user IS NOT NULL))
```

#### Item c

I'm assuming that when the item mentions _"Get all transactions from yesterday"_,
this means transactions that were _certified_ yesterday, and not _created_ yesterday,
since we want to filter on amount of time between `created` and `certified_by_user`, and
from the item we see that could also be 3 days.

Now, for this use-case, since we are only concerned with data from yesterday,
we can make use of a great feature introduced in Postgres 9.5 called BRIN, or
Block Range INdexes (https://wiki.postgresql.org/wiki/What's_new_in_PostgreSQL_9.5#BRIN_Indexes).

This allows the index to skip the entire 99%+ of the table that won't be considered by the query,
while at the same time bring far smaller disk-space-wise.

Important note: at the time of testing, my new `transactions` table was only synchronized
with data up until August 12, 2018, and so the test query and explain outputs show here were executed
accordingly (as if "yesterday" = August 11, 2018). This will better reflect execution costs,
as opposed to using the actual yesterday date, since there would be 0 records then.

Index:

```sql
create index certified_transactions_time_interval_abs_amount_idx
  on transactions USING BRIN (certified_by_user,
                              abs(amount),
                              ((certified_by_user - created)/1000 * '1 second'::interval));
```

Test query:
```sql
select * from rome.public.transactions
where abs(amount) >= 1000
  and (certified_by_user - created)/1000 * '1 second'::interval > interval '10 hours'
  and certified_by_user >= 1534009981334;
```

Explain before:
```sql
Seq Scan on transactions  (cost=0.00..1582937.44 rows=592989 width=70)
  Filter: ((certified_by_user >= '1533945600000'::bigint) AND (abs(amount) >= '1000'::numeric) AND (((((certified_by_user - created) / 1000))::double precision * '00:00:01'::interval) > '10:00:00'::interval))
```

Explain after:
```sql
Bitmap Heap Scan on transactions  (cost=7712.61..512479.20 rows=592989 width=70)
  Recheck Cond: ((certified_by_user >= '1533945600000'::bigint) AND (abs(amount) >= '1000'::numeric) AND (((((certified_by_user - created) / 1000))::double precision * '00:00:01'::interval) > '10:00:00'::interval))
  ->  Bitmap Index Scan on certified_transactions_time_interval_abs_amount_idx  (cost=0.00..7564.36 rows=592989 width=0)
        Index Cond: ((certified_by_user >= '1533945600000'::bigint) AND (abs(amount) >= '1000'::numeric) AND (((((certified_by_user - created) / 1000))::double precision * '00:00:01'::interval) > '10:00:00'::interval))
```


## Minimum Requirements

- Python 3.7.2
