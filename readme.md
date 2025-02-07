The code in this repository was created in 2021.

# Description
The purpose of this library (PostgreSqlCopyUtil) is to stream large amounts of data into a PostgreSQL database table without loading the entire dataset into memory, preventing OutOfMemoryError exceptions.

The underlying implementation uses the
<a href="https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html">CopyManager</a>,
which leverages PostgreSQL’s <a href="https://www.postgresql.org/docs/17/sql-copy.html">COPY</a> command.