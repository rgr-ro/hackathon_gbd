# PostgreSQL + pgAdmin (docker-compose)

This repo adds a simple `docker-compose.yml` that launches:

- PostgreSQL (service name: `db`) with persistent volume
- pgAdmin (web UI) accessible from your host at http://localhost:8080

Defaults (change before production):
- PostgreSQL user: `myuser`
- PostgreSQL password: `mypassword`
- PostgreSQL database: `mydb`
- pgAdmin email: `admin@example.com`
- pgAdmin password: `admin`

Files created:
- `docker-compose.yml` — launch postgres + pgAdmin

Quick start (PowerShell / Windows):

1) From this folder run:

```powershell
# If you have modern Docker CLI
docker compose up -d

# or, if you have the standalone docker-compose installed
# docker-compose up -d
```

2) Open the pgAdmin UI in your browser:

    http://localhost:8080

3) Log in using the pgAdmin credentials above (`admin@example.com` / `admin`).

4) Add a new server in pgAdmin (GUI -> Add Server):
   - General > Name: choose anything (e.g. My Postgres)
   - Connection:
     - Host: `db` (the service name; pgAdmin container can resolve it on the same Docker network)
     - Port: `5432`
     - Username: `myuser`
     - Password: `mypassword`

pgvector support
----------------

This compose file can be built so PostgreSQL comes with the `pgvector` extension installed.

How it works
- The `db` service is built from `./db/Dockerfile`. That Dockerfile compiles and installs the pgvector extension into the Postgres image at build time.

Enable and verify pgvector
1) Build and start the stack (PowerShell):

```powershell
# Builds the custom DB image (with pgvector) and starts the services
docker compose up -d --build
```

2) Create the extension inside a database (either via pgAdmin query tool, or psql):

Using psql from the host into the running container:

```powershell
docker compose exec db psql -U myuser -d mydb -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

Or, using the DB query tool inside pgAdmin, run:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

3) Verify it exists:

```powershell
docker compose exec db psql -U myuser -d mydb -c "\dx"
# Look for pgvector in the listed extensions
```

Quick usage example (SQL):

```sql
-- create a table with a vector column (dimension 1536 is example)
CREATE TABLE items (id serial PRIMARY KEY, embedding vector(1536));

-- insert a vector (example values)
INSERT INTO items (embedding) VALUES ('[0.1,0.2,0.3,...]');

-- index for fast nearest-neighbor search
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);

-- search
SELECT id, embedding <-> '[0.1,0.2,0.3,...]' AS distance FROM items ORDER BY distance LIMIT 5;
```

Expected result for the example KNN query
----------------------------------------

Given the example data (one row inserted with embedding '[0.1,0.2,0.3,0.4,0.5]') and the query using the same vector:

- The KNN operator (<->) returns the Euclidean (L2) distance.
- For an identical vector the distance is 0.
- With ORDER BY distance LIMIT 5 the identical row is returned first.

Example psql-style output for the provided SQL (assuming the inserted row has id = 1):

```text
 id | distance
----+----------
  1 |        0
(1 row)
```

If you insert additional rows, expect positive numeric distances for non-identical vectors, sorted ascending (closest first).

Quick commands for a clean PostgreSQL instance
---------------------------------------------

1) Using psql (from host or container)
- Create a user and database (run as postgres superuser):

```bash
# from host (adjust host/port/user as needed)
psql -h localhost -p 5432 -U postgres -c "CREATE USER myuser WITH PASSWORD 'mypassword';"
psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE mydb OWNER myuser;"
```

- Connect and enable pgvector, create table, insert, index, and test:

```bash
# connect to mydb as myuser (or run the following SQL via pgAdmin Query Tool)
psql -h localhost -p 5432 -U myuser -d mydb
```

```sql
-- SQL to run in psql or pgAdmin Query Tool
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS items (id serial PRIMARY KEY, embedding vector(5));
INSERT INTO items (embedding) VALUES ('[0.1,0.2,0.3,0.4,0.5]'); -- example 5-dim vector; match your dimension
CREATE INDEX IF NOT EXISTS items_embedding_idx ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
-- nearest neighbour test
SELECT id, embedding <-> '[0.1,0.2,0.3,0.4,0.5]' AS distance
FROM items
ORDER BY distance
LIMIT 5;
```

2) Using docker compose exec (if running via the included compose)
```bash
# create extension inside container non-interactively (PowerShell example)
docker compose exec db bash -c 'PGPASSWORD="mypassword" psql -U myuser -d mydb -c "CREATE EXTENSION IF NOT EXISTS vector;"'
```

3) Using pgAdmin (GUI)
- Add or edit a server:
  - Host: db (or localhost if connecting from host), Port: 5432
  - Username: myuser, Password: mypassword (save if desired)
- Open the Query Tool for your database (Tools -> Query Tool) and run the SQL block above to create the extension, table, insert sample, build index, and run the test query.

Verify
- List extensions to confirm pgvector installed:
```bash
# in psql
\dx
```
- Query results should return the inserted row and a numeric distance value.

Notes
- Ensure the vector dimension in the schema and inserted vectors match (example uses 1536 in other parts of the README; the minimal sample above uses 5 dims — adjust as needed).
- If using a prebuilt image that already has pgvector, you only need to CREATE EXTENSION in the target database.

Troubleshooting
- To see logs:

```powershell
docker compose logs -f
# or for a single service:
docker compose logs -f pgadmin
```

- To stop and remove containers:

```powershell
docker compose down
```

Common connection error: "no password supplied"
------------------------------------------------

If you see an error like:

```
connection failed: connection to server at "172.19.0.2", port 5432 failed: fe_sendauth: no password supplied
```

It means the client attempted password authentication but no password was provided. Fixes below:

pgAdmin
- When adding or editing the server in pgAdmin (right-click server -> Properties -> Connection):
  - Host name/address: `db` (or container IP, but `db` is preferred)
  - Port: `5432`
  - Username: `myuser`
  - Password: `mypassword` (enter and check "Save Password" to persist)

psql from PowerShell (host)
- Set the `PGPASSWORD` environment variable for the session and run psql. Example (PowerShell):

```powershell
$env:PGPASSWORD = 'mypassword'
psql -h localhost -p 5432 -U myuser -d mydb -c "SELECT version();"
# remove the env var when done
Remove-Item Env:PGPASSWORD
```

Or use a connection string (note: may expose password in history):

```powershell
psql "postgresql://myuser:mypassword@localhost:5432/mydb" -c "SELECT 1;"
```

psql inside the container
- Run psql from inside the `db` container and pass PGPASSWORD in the command (non-interactive):

```powershell
# Correct PowerShell-friendly way (recommended)
# Use single quotes for the bash -c argument so PowerShell doesn't interpret backslashes/escapes.
docker compose exec db bash -c 'PGPASSWORD="mypassword" psql -U myuser -d mydb -c "CREATE EXTENSION IF NOT EXISTS vector;"'

# Alternative: run psql as the default `postgres` user (no password) and target your DB
# This works because internal local connections in the container often allow the postgres superuser
# to connect without a password. Only use this if you know the `postgres` user is available.
docker compose exec db psql -U postgres -d mydb -c "CREATE EXTENSION IF NOT EXISTS vector;"

# Explanation of the original error
# The previous README example used backslashes to escape quotes intended for bash, but when that
# line was pasted into PowerShell the backslashes and nested quotes were interpreted by PowerShell
# itself. PowerShell then tried to execute a stray backslash or incorrectly parsed token, producing
# the message: "The term '\' is not recognized as the name of a cmdlet...".
#
# Using the single-quoted outer string ensures PowerShell passes the inner text verbatim to bash -c.
```

If you still see authentication errors, check Postgres logs and `pg_hba.conf` inside the container to confirm the required auth method (`md5`/`scram-sha-256`) and ensure the username/password match.

Customization
- Change ports in `docker-compose.yml` if 8080 or 5432 conflict with local services.
- To preconfigure servers in pgAdmin automatically, you can mount a `servers.json` into pgAdmin's config directory; consult the dpage/pgadmin4 docs for details.
