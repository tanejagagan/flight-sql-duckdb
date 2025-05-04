# Flight-Sql-Duckdb

## An [Arrow Flight SQL Server](https://arrow.apache.org/docs/format/FlightSql.html) with [DuckDB](https://duckdb.org) back-end execution engines

[<img src="https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker">](https://hub.docker.com/r/voltrondata/sqlflite)
[<img src="https://img.shields.io/badge/Documentation-dev-yellow.svg?logo=">](https://arrow.apache.org/docs/format/FlightSql.html)
[<img src="https://img.shields.io/badge/Arrow%20JDBC%20Driver-download%20artifact-red?logo=Apache%20Maven">](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)
[<img src="https://img.shields.io/badge/PyPI-Arrow%20ADBC%20Flight%20SQL%20driver-blue?logo=PyPI">](https://pypi.org/project/adbc-driver-flightsql/)
[<img src="https://img.shields.io/badge/PyPI-SQLFlite%20Ibis%20Backend-blue?logo=PyPI">](https://pypi.org/project/ibis-sqlflite/)
[<img src="https://img.shields.io/badge/PyPI-SQLFlite%20SQLAlchemy%20Dialect-blue?logo=PyPI">](https://pypi.org/project/sqlalchemy-sqlflite-adbc-dialect/)
<br> Flight Sql Server with DuckDB backend lets you run DuckDB remotely and let multiple user connect to it remotely with flight jdbc driver.
<br> It's very similar to https://github.com/voltrondata/sqlflite <br> But written in Java which for those comfortable writing code in java.
This support all the clients including JDBC, ADBC Python flight sql driver as well as sqlflite_client CLI tool
## Dev Setup
Requirement
JDK 17 or 21

### Getting started with Docker
- Build the docker image with
  `./mvnw clean package -DskipTests jib:dockerBuild`
- Start the container with `example/data` mounted to the container
  ` docker run -ti -v "$PWD/example/data":/data -p 55559:55559  flight-sql-duckdb`

### Connecting to the server via JDBC
Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)

### Support functionality
1. Database and schema specified as part of connection url. Passed to server as header database and schema.
2. Fetch size can be specified. It's passed to the server in header fetch_size.
3. Bulk write to parquet file using bulk upload functionality. Idea is to to bulk upload and then add those files to metadata.
4. Username and Passwords can be specified in application.conf file.

You can then use the JDBC driver to connect from your host computer to the locally running Docker Flight SQL server with this JDBC string (change the password value to match the value specified for the SQLFLITE_PASSWORD environment variable if you changed it from the example above):
```bash
jdbc:arrow-flight-sql://localhost:55559??database=memory&useEncryption=0&user=admin&password=admin
```

For instructions on setting up the JDBC driver in popular Database IDE tool: [DBeaver Community Edition](https://dbeaver.io) - see this [repo](https://github.com/voltrondata/setup-arrow-jdbc-driver-in-dbeaver).

**Note** - if you stop/restart the Flight SQL Docker container, and attempt to connect via JDBC with the same password - you could get error: "Invalid bearer token provided. Detail: Unauthenticated".  This is because the client JDBC driver caches the bearer token signed with the previous instance's secret key.  Just change the password in the new container by changing the "SQLFLITE_PASSWORD" env var setting - and then use that to connect via JDBC.

### Connecting to the server via the new [ADBC Python Flight SQL driver](https://pypi.org/project/adbc-driver-flightsql/)

You can now use the new Apache Arrow Python ADBC Flight SQL driver to query the Flight SQL server.  ADBC offers performance advantages over JDBC - because it minimizes serialization/deserialization, and data stays in columnar format at all phases.

You can learn more about ADBC and Flight SQL [here](https://voltrondata.com/resources/simplifying-database-connectivity-with-arrow-flight-sql-and-adbc).

Ensure you have Python 3.9+ installed, then open a terminal, then run:
```bash
# Create a Python virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Install the requirements including the new Arrow ADBC Flight SQL driver
pip install --upgrade pip
pip install pandas pyarrow adbc_driver_flightsql

# Start the python interactive shell
python
```

In the Python shell - you can then run:
```python
import os
from adbc_driver_flightsql import dbapi as sqlflite, DatabaseOptions


with sqlflite.connect(uri="grpc+tls://localhost:55559",
                        db_kwargs={"username": os.getenv("SQLFLITE_USERNAME", "admin"),
                                   "password": os.getenv("SQLFLITE_PASSWORD", "admin"),
                                   DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Not needed if you use a trusted CA-signed TLS cert
                                   }
                        ) as conn:
   with conn.cursor() as cur:
       cur.execute("select * from generate_series(20)",
                   )
       x = cur.fetch_arrow_table()
       print(x)
```

You should see results:


### Connecting via [Ibis](https://ibis-project.org)
See: https://github.com/ibis-project/ibis-sqlflite

### Connecting via [SQLAlchemy](https://www.sqlalchemy.org)
See: https://github.com/prmoore77/sqlalchemy-sqlflite-adbc-dialect
