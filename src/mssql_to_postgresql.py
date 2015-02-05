# Using http://www.sqlines.com/sql-server-to-postgresql
# Usage: python mssql_to_postgresql.py <<input file>> <<output file>>

import sys
mappings = {'BINARY(n)': 'BYTEA',
            'BIT': 'BOOLEAN',
            'DATETIME': 'TIMESTAMP(3)',
            'DATETIME2(p)': 'TIMESTAMP(p)',
            'DATETIMEOFFSET(p)': 'TIMESTAMP(p) WITH TIME ZONE',
            'FLOAT(p)': 'DOUBLE PRECISION',
            'IMAGE': 'BYTEA',
            'NCHAR(n)': 'CHAR(n)',
            'NTEXT': 'TEXT',
            'NVARCHAR(max)': 'TEXT',
            'NVARCHAR(n)': 'VARCHAR(n)',
            'ROWVERSION': 'BYTEA',
            'SMALLDATETIME': 'TIMESTAMP(0)',
            'SMALLMONEY': 'MONEY',
            'TIMESTAMP': 'BYTEA',
            'TINYINT': 'SMALLINT',
            'UNIQUEIDENTIFIER': 'CHAR(16)',
            'VARBINARY(max)': 'BYTEA',
            'VARBINARY(n)': 'BYTEA',
            'VARCHAR(max)': 'TEXT'}


def replace_all(string, mappings):
    s = ''.join(string).upper()
    # Remove [, ], K12INTEL_DW and the . from input file
    s = s.replace('[', '').replace(']', '')
    s = s.split('.')[1]
    # Map MSSQL types to PGSQL types
    for mssql, pgsql in mappings.iteritems():
        s = s.replace(mssql, pgsql)
    return s


if __name__ == '__main__':

    to_convert_filename = sys.argv[1]
    out_filename = sys.argv[2]

    with open(to_convert_filename) as f:
        text = f.readlines()
        converted = replace_all(text, mappings)
        outfile = open(out_filename, 'wb+')
        outfile.write(converted)
        outfile.close()
