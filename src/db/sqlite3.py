import sqlite3
from typing import Any
from .db_abc import DB


class Sqlite3DB(DB):
    def __init__(self, path: str = "db.sqlite3"):
        self.conn = sqlite3.connect(path)
        self.__create_tables()

    def __del__(self):
        self.conn.close()

    def __create_tables(self):
        cursor = self.conn.cursor()
        # cursor.execute('PRAGMA foreign_keys = ON')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status_type TEXT NOT NULL,
            Exit_status INTEGER,
            array_indices TEXT,
            'Resource_List.ctype' TEXT,
            'Resource_List.mem' TEXT,
            'Resource_List.mpiprocs' INTEGER,
            'Resource_List.naccelerators' INTEGER,
            'Resource_List.ncpus' INTEGER,
            'Resource_List.nodect' INTEGER,
            'Resource_List.nodes' TEXT,
            'Resource_List.place' TEXT,
            'Resource_List.psets' TEXT,
            'Resource_List.qprio' TEXT,
            'Resource_List.select' TEXT,
            'Resource_List.vntype' TEXT,
            'Resource_List.walltime' TEXT,
            ctime INTEGER,
            end INTEGER,
            etime INTEGER,
            depend TEXT,
            exec_host TEXT,
            exec_vnode TEXT,
            'group' TEXT,
            jobname TEXT,
            project TEXT,
            accounting_id TEXT,
            qtime INTEGER,
            queue TEXT,
            'resource_assigned.mem' TEXT,
            'resource_assigned.ncpus' INTEGER,
            'resource_assigned.ngpus' INTEGER,
            'resources_used.cpupercent' INTEGER,
            'resources_used.cput' TEXT,
            'resources_used.mem' TEXT,
            'resources_used.ncpus' INTEGER,
            'resources_used.vmem' TEXT,
            'resources_used.walltime' TEXT,
            requestor TEXT,
            run_count INTEGER,
            session INTEGER,
            alt_id INTEGER,
            start INTEGER,
            user TEXT
            )
        ''')
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS jobs (
        #     id TEXT PRIMARY KEY,
        #     status_type TEXT NOT NULL,
        #     Exit_status INTEGER,
        #     Resource_List.ctype TEXT,
        #     Resource_List.mem TEXT,
        #     Resource_List.mpiprocs INTEGER,
        #     Resource_List.naccelerators INTEGER,
        #     Resource_List.ncpus INTEGER,
        #     Resource_List.nodect INTEGER,
        #     Resource_List.nodes TEXT,
        #     Resource_List.place TEXT,
        #     Resource_List.psets TEXT,
        #     Resource_List.qprio TEXT,
        #     Resource_List.select TEXT,
        #     Resource_List.vntype TEXT,
        #     Resource_List.walltime TEXT,
        #     ctime INTEGER,
        #     end INTEGER,
        #     etime INTEGER,
        #     exec_host TEXT,
        #     exec_vnode TEXT,
        #     group TEXT,
        #     jobname TEXT,
        #     project TEXT,
        #     qtime INTEGER,
        #     queue TEXT,
        #     resource_assigned.mem TEXT,
        #     resource_assigned.ncpus INTEGER,
        #     resource_assigned.ngpus INTEGER,
        #     resources_used.cpupercent INTEGER,
        #     resources_used.cput TEXT,
        #     resources_used.mem TEXT,
        #     resources_used.ncpus INTEGER,
        #     resources_used.vmem TEXT,
        #     resources_used.walltime TEXT,
        #     run_count INTEGER,
        #     session INTEGER,
        #     start INTEGER,
        #     user TEXT
        #     )
        # ''')
        self.conn.commit()

    def create_job(self, job_id: str, values: dict[str: Any]):
        column_order = "'id', "
        insert_values = [job_id]

        for k, v in values.items():
            column_order += f" '{k}',"
            insert_values.append(v)
        column_order = column_order[:-1]
        values_questions = ','.join(['?']*len(insert_values))

        cursor = self.conn.cursor()
        cursor.execute(
            f"INSERT INTO jobs({column_order}) VALUES({values_questions})",
            insert_values
        )
        self.conn.commit()
        cursor.close()

    def update_job(self, job_id: str, values: dict[str: Any]):
        column_order = ""
        insert_values = []

        for k, v in values.items():
            column_order += f" '{k}' = ?,"
            insert_values.append(v)
        column_order = column_order[:-1]

        cursor = self.conn.cursor()
        query = f"UPDATE jobs SET {column_order} WHERE id = '{job_id}'"
        cursor.execute(
            query,
            insert_values
        )
        self.conn.commit()

        res = cursor.execute(f"SELECT status_type FROM jobs WHERE id='{job_id}'")
        job_status = res.fetchone()[0]
        assert job_status == values["status_type"]
        cursor.close()

    def has_job(self, job_id: str) -> bool:
        cursor = self.conn.cursor()
        res = cursor.execute(f"SELECT id FROM jobs WHERE id='{job_id}'")
        job_exist = res.fetchone()
        cursor.close()
        return job_exist is not None
