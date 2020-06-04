# SQL help

The common things, actions, etc. to use e2db effectively.

Some common SQL things to use with e2db

Connecting from `psql`:

```
psql --username=postgres --host=eth2db1.cknjc1myjw5g.us-east-1.rds.amazonaws.com
```

Connecting in python:
```
--db-addr=postgresql+psycopg2://username:password@eth2db1.cknjc1myjw5g.us-east-1.rds.amazonaws.com/witti
```

Creating a database for a testnet

```
CREATE DATABASE witti;
```

Commons:

```
\l
\d
```

Creating a read-only user `eth2reader`:

```
CREATE USER eth2reader WITH PASSWORD 'passwordhere';

\c witti

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO eth2reader;
GRANT USAGE ON SCHEMA public TO eth2reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO eth2reader;
```

Working with binary blobs as hex:

```
INSERT INTO
  mytable (testcol)
VALUES
  (decode('abcd1234deadbeef', 'hex'))
```
