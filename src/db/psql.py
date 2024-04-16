from typing import Any
import psycopg
from .db_abc import DB


class PSQL(DB):

    def __init__(
            self,
            user: str = "postgres",
            password: str = "postgres",
            host: str = "localhost",
            port: int = 5432,
            dbname: str = "pbs_ca_log_project",
            table_name: str = "acc_jobs"
    ) -> None:

        self.table_name = table_name
        self.conn = psycopg.connect(f"user={user} password={password} host={host} port={port} dbname={dbname}")
        self.cursor = self.conn.cursor()
        with self.conn.cursor() as cursor:  # CONNECTION CHECK
            res = cursor.execute("SELECT current_user")
            current_user = res.fetchone()
            if current_user is not None:
                print("PSQL CONNECTION ESTABLISHED")

        self.__create_tables()

        self.timestamp_without_time_zone_keys = [
            'ctime',
            'end',
            'etime',
            'qtime',
            'start',
        ]

    def __del__(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def __create_tables(self):
        with self.conn.cursor() as cursor:
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                id TEXT PRIMARY KEY,
                status_type TEXT NOT NULL,
                "Exit_status" INTEGER,
                array_indices TEXT,
                "Resource_List.ctype" TEXT,
                "Resource_List.mem" TEXT,
                "Resource_List.mpiprocs" INTEGER,
                "Resource_List.naccelerators" INTEGER,
                "Resource_List.ncpus" INTEGER,
                "Resource_List.nodect" INTEGER,
                "Resource_List.nodes" TEXT,
                "Resource_List.place" TEXT,
                "Resource_List.psets" TEXT,
                "Resource_List.qprio" TEXT,
                "Resource_List.select" TEXT,
                "Resource_List.vntype" TEXT,
                "Resource_List.walltime" BIGINT,
                ctime timestamp without time zone,
                eligible_time BIGINT,
                "end" timestamp without time zone,
                etime timestamp without time zone,
                depend TEXT,
                exec_host TEXT,
                exec_vnode TEXT,
                "group" TEXT,
                jobname TEXT,
                pcap_accelerator INTEGER,
                pcap_node INTEGER,
                pgov TEXT,
                project TEXT,
                account TEXT,
                accounting_id TEXT,
                qtime timestamp without time zone,
                queue TEXT,
                "resource_assigned.mem" TEXT,
                "resource_assigned.ncpus" INTEGER,
                "resource_assigned.ngpus" INTEGER,
                "resources_used.cpupercent" INTEGER,
                "resources_used.cput" BIGINT,
                "resources_used.mem" TEXT,
                "resources_used.ncpus" INTEGER,
                "resources_used.vmem" TEXT,
                "resources_used.walltime" BIGINT,
                resvID TEXT,
                resvname TEXT,
                requestor TEXT,
                run_count INTEGER,
                session INTEGER,
                alt_id INTEGER,
                start timestamp without time zone,
                "user" TEXT
                )
            ''')
            self.conn.commit()

    def create_job(self, job_id: str, values: dict[str: Any]):
        column_order = '"id"'
        values_questions = '%s'
        insert_values = [job_id]

        for k, v in values.items():
            column_order += f', "{k}"'
            if k in self.timestamp_without_time_zone_keys:
                values_questions += ', to_timestamp(%s)'
            else:
                values_questions += ', %s'
            insert_values.append(v)

        with self.conn.transaction():
            self.cursor.execute(
                f'INSERT INTO {self.table_name}({column_order}) VALUES({values_questions})',
                insert_values
            )
            # self.conn.commit()

    def update_job(self, job_id: str, values: dict[str: Any]):
        column_order = ''
        insert_values = []

        for k, v in values.items():
            if k in self.timestamp_without_time_zone_keys:
                column_order += f' "{k}" = to_timestamp(%s),'
            else:
                column_order += f' "{k}" = %s,'
            insert_values.append(v)
        column_order = column_order[:-1]
        query = f"UPDATE {self.table_name} SET {column_order} WHERE id = '{job_id}'"

        with self.conn.transaction():
            self.cursor.execute(
                query,
                insert_values
            )
            # self.conn.commit()

        # res = cursor.execute(f"SELECT status_type FROM {self.table_name} WHERE id='{job_id}'")
        # job_status = res.fetchone()[0]
        # assert job_status == values["status_type"]

    def has_job(self, job_id: str) -> bool:
        with self.conn.transaction():
            res = self.cursor.execute(f"SELECT id FROM {self.table_name} WHERE id='{job_id}'")
            job_exist = res.fetchone()
            return job_exist is not None
