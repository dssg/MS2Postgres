# ms2pg.py
## Moves SQL Server tables to PostgreSQL
## Description
Moves the data in an existing table in a SQL Server to data in an existing
table in a PostgreSQL server. Currently requires that a table with the same
name and compatible schema exist on both the SQL Server and the PostgreSQL
server. By "same name" I mean that the Postgres table has the same name as the
SQL Server table but does not include the SQL Server owner. For example,
[dbo].[employees] in SQL Server should be employees in PostgreSQL.

## Usage
python ms2pg.py ms_con_string pgres_con_string table_name ord_col

###arguments:
* ms_con_string is the ODBC connection string for the SQL Server
* pgres_con_string is the ODBC connection string for the PostgreSQL server
* table_name is the name of the table to move (according to SQL Server)
* ord_col is the name of a column by which the table can be ordered.


###example:
python src/ms2pg.py DSN=SQL_SERVER_DSN DSN=PostgreSQL_DSN employees employee_id


