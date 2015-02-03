import pyobdc
import sys

Class Mover:
    def __init__(self, ms_con_str, pgres_con_str, log_file = None, 
        commit_freq = 1000):
        self.__cxn_ms = pyobdc.connect(ms_con_str)
        self__.cxn_pgres = pyobdc.connect(pgres_con_str)
        if log_file is None:
            self.__log = sys.stdout
            self.__log_is_file = False
        else:
            self.__log = open(log_file)
            self.__log_is_file = True
        self.__commit_freq = commit_freq

    def __log(self, comment):
        """writes a comment to the log, then commits it"""
        # stub
        pass

    def __create_table_if_not_exists(self, ms_table_name, ms_schema = None):
        """Builds table if necessary, returns (pgres table name, column names)

        """ 
        # stub
        return ms_table_name, (,) 

    def move_table(self, ms_table_name, ordr_col=None, ms_schema = None):
        """Moves a table from SQL Server to Postgres server

           arguments:
           ms_table_name -- The name of the table in SQL Server
           ordr_col -- Column used to order the table. This is important
                       if the move gets interrupted and you need to restart
                       from the middle
           ms_schema -- Query that SQL server uses to create table

        """   
        #TODO use ROW_NUMBER() so we can start later in the table
        pgres_table_name, columns = __create_table_if_not_exists(
            ms_table_name, ms_schema)        

        from_cursor = self.__cxn_ms.cursor()
        sql = "SELECT * FROM {}".format(ms_table_name)
        if ordr_col is not None:
            sql += " ORDER BY {}".format(ordr_col)
        from_cursor.execute(sql) 

        to_cursor = self.__cxn_pgres.cursor()
        records_processed = 0
        sql = "INSERT INTO {} ({}) VALUES ({});".format(pgres_table_name,
            ", ".join(colums), ", ".join("?" * len(columns)))

        __log("Starting move: {} -> {}\n".format(ms_table_name, 
            pgres_table_name))
        for row in from_cursor:
            to_cursor.execute(sql, *row)
            records_processed += 1
            if records_processed % self.__commit_freq == 0:
                to_cursor.commit()
                __log("Moved {} rows\n".format(records_processed))
        to_cursor.commit()
        __log("Complete: moved {} rows.".format(records_processed))

        
                                        
                

