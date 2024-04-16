from src.acclog import AccountingLog
from src.db.psql import PSQL
import sys
import getopt

HELP_STR = f'''
Usage: {sys.argv[0]} [OPTIONS]... <file>
OPTIONS:
    --user - PSQL user. DEFAULT: postgres
    --password - PSQL password. DEFAULT: postgres
    --host - PSQL host. DEFAULT: 127.0.0.1
    --port - PSQL port. DEFAULT: 5432
    --dbname - PSQL Database name. DEFAULT: acclogdb
    --table_name - PSQL table name in PSQL Database (--dbname). DEFAULT: accjobs
'''


if __name__ == "__main__":

    file_path = ''
    PG_USER = 'postgres'
    PG_PASSWORD = 'postgres'
    PG_HOST = '127.0.0.1'
    PG_PORT = 5432
    PG_DBNAME: str = "acclogdb"
    PG_TABLE: str = "accjobs"

    short_options = "h"
    long_options = ["user=", "password=", "host=", "port=", "dbname=", "table_name="]

    try:
        opts, args = getopt.getopt(sys.argv[1:], short_options, long_options)
        # print(opts)
    except getopt.GetoptError:
        print(HELP_STR)
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(HELP_STR)
            sys.exit()
        elif opt in ("--user"):
            PG_USER = arg
        elif opt in ("--password"):
            PG_PASSWORD = arg
        elif opt in ("--host"):
            PG_HOST = arg
        elif opt in ("--port"):
            PG_PORT = int(arg)
        elif opt in ("--dbname"):
            PG_DBNAME = arg
        elif opt in ("--table_name"):
            PG_TABLE = arg

    if args:
        file_path = args[0]
    else:
        print(HELP_STR)
        exit(2)

    sql = PSQL(
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DBNAME,
        table_name=PG_TABLE,
    )

    al = AccountingLog(db=sql)  # db=sql
    al.analyze(file_path)
