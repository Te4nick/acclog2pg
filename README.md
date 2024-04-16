# acclog2pg
## Summary
Python command-line tool to parse and post OpenPBS Accounting logs to PostgreSQL Database.
!!! LINUX ONLY !!!
Based on OpenPBS Python Test Library. All credits to code in `ptl` package goes to OpenPBS.

## Running
- Get Python >= 3.6, psycopg~=3.1.18 (requirements.txt included)
  ```bash
  $ pip install -r requirements.txt
  ```
- Run like any other python script
  ```bash
  $ python acclog2pg.py /path/to/accounting/log

## Usage
```bash
$ python acclog2pg.py -h
Usage: python acclog2pg.py [OPTIONS]... <file>
OPTIONS:
    --user - PSQL user. DEFAULT: postgres
    --password - PSQL password. DEFAULT: postgres
    --host - PSQL host. DEFAULT: 127.0.0.1
    --port - PSQL port. DEFAULT: 5432
    --dbname - PSQL Database name. DEFAULT: acclogdb
    --table_name - PSQL table name in PSQL Database (--dbname). DEFAULT: accjobs
```
