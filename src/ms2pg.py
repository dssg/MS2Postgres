import pyodbc
import sys
import os
import time
from schemas import mappings, replace_all as convert_schema

class Mover:
    """Object to move tables between two databases. 

    Keeps track of the two database connections, the log file, and the
    commit frequency.
    """


    def __init__(self, ms_con_str, pgres_con_str, log_file = None, 
        commit_freq = 1000):
        """initializer

        arguments:
        ms_con_str -- string to connect to SQL Server database
        pgres_con_str -- string to connect to PostgreSQL database
        log_file -- path of file to log transactions. Use None to turn off
           logging.
        commit_freq -- number of rows to move before committing and logging
            each transaction
        
        """      

        self.__cxn_ms = pyodbc.connect(ms_con_str)
        self.__cxn_pgres = pyodbc.connect(pgres_con_str)
        if log_file is not None:
            self.__log_file = open(log_file, 'a')
            self.__log_is_file = True
        self.__commit_freq = commit_freq
        self.__log('Connection initialized:')
        self.__log('SQL Server Connection String: {}'.format(ms_con_str))
        self.__log('PostgreSQL Connection String: {}'.format(pgres_con_str))
        self.__log('')
        self.__log('Commit Frequency: {}'.format(commit_freq))
        self.__log('')

    def __log(self, comment):
        """writes a comment to the log, then commits it"""
        w_ts = '{}: {}'.format(time.strftime('%m/%d/%Y %H:%M:%S'), 
            comment)
        print w_ts
        if self.__log_is_file:
            self.__log_file.write(w_ts + '\n')
            self.__log_file.flush()
            os.fsync(self.__log_file.fileno())


    def __create_table_if_not_exists(self, ms_table_name, ms_schema = None):
        """Builds table if necessary, returns (pgres table name, column names)

        """ 
        if ms_schema is None:
            self.__log('No schema provided. Assuming table already exists'
                ' in postgres')
            pgres_table_name = ms_table_name
        else:
            pgres_schema = convert_schema(ms_schema, mappings)
            pgres_table_name = pgres_schema.split('(')[0]
            create_query = ('CREATE TABLE IF NOT EXISTS ' +
                pgres_schema)
            cur.execute(create_query)
            cur.commit()
            self.__log('SQL: {}'.format(create_query))
        cur = self.__cxn_pgres.cursor()
        col_names = map(lambda row: row.column_name, 
            cur.columns(table = pgres_table_name))
        return (pgres_table_name, col_names)

    def move_table(self, ms_table_name, ordr_col, ms_schema = None):
        """Moves a table from SQL Server to Postgres server

        arguments:
        ms_table_name -- The name of the table in SQL Server
        ordr_col -- Column used to order the table. This is important
                    if the move gets interrupted and you need to restart
                    from the middle
        ms_schema -- Query that SQL server uses to create table

        """   
        #TODO use ROW_NUMBER() so we can start later in the table
        self.__log('Moving table')
        self.__log('SQL Server table name: {}'.format(ms_table_name))
        self.__log('Order Col: {}'.format(ordr_col))
        self.__log('SQL Server schema: {}'.format(ms_schema))
        pgres_table_name, columns = self.__create_table_if_not_exists(
            ms_table_name, ms_schema)        
        from_cursor = self.__cxn_ms.cursor()
        sql = "SELECT * FROM {} ORDER BY {}".format(ms_table_name, ordr_col)
        from_cursor.execute(sql) 
        self.__log('SQL: {}'.format(sql))
        to_cursor = self.__cxn_pgres.cursor()
        records_processed = 0
        sql = "INSERT INTO {} ({}) VALUES ({});".format(pgres_table_name,
            ", ".join(columns), ", ".join("?" * len(columns)))

        self.__log("Starting move: {} -> {}\n".format(ms_table_name, 
            pgres_table_name))
        self.__log('SQL: {}'.format(sql))
        for row in from_cursor:
            to_cursor.execute(sql, *row)
            records_processed += 1
            if records_processed % self.__commit_freq == 0:
                to_cursor.commit()
                self.__log("Moved {} rows\n".format(records_processed))
        to_cursor.commit()
        self.__log('Complete: moved {} rows.'.format(records_processed))
        self.__log('')

if __name__ == '__main__':
    #Run:
    #python ms2pg.py ms_con_string pgres_con_string table_name ord_col

    m = Mover(sys.argv[1], sys.argv[2], 'ms2pg.log')
    m.move_table(sys.argv[3], sys.argv[4])    

