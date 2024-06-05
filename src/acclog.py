from ptl.utils.pbs_logutils import PBSAccountingLog, PARSER_OK_CONTINUE
from src.db import DB


class AccountingLog(PBSAccountingLog):

    def __init__(self, filename=None, hostname=None, show_progress=False, db: DB = None):
        super().__init__(filename, hostname, show_progress)
        self.job_info_res = {}  # self.enable_job_info_parsing()
        self.job_info_parsing = True
        self.__job_db = db

        self.int_keys = [
            'Exit_status',
            'Resource_List.mpiprocs',
            'Resource_List.naccelerators',
            'Resource_List.ncpus',
            'Resource_List.nodect',
            'pcap_accelerator',
            'pcap_node',
            'resource_assigned.ncpus',
            'resource_assigned.ngpus',
            'resources_used.cpupercent',
            'resources_used.ncpus',
            'run_count',
            'session',
            'alt_id',
            # TIMES:
            'ctime',
            'end',
            'etime',
            'qtime',
            'start',
        ]
        self.walltime_to_seconds_keys = [
            'resources_used.walltime',
            'Resource_List.walltime',
            'resources_used.cput',
            'eligible_time',
        ]

        self.banned_columns = {
            'delect': True,
        }

    def analyze(self, path=None, start=None, end=None, hostname=None,
                summarize=True, sudo=False):
        res = super().analyze(path, start, end, hostname, summarize, sudo)

        if not (self.__job_db is None):  # Put to DB
            for job_id, values in self.job_info_res.items():
                if self.__job_db.has_job(job_id):
                    self.__job_db.update_job(job_id, values)
                else:
                    self.__job_db.create_job(job_id, values)

        return res

    def job_info(self, rec):
        """
        PBS Job information
        """
        allowed_types = ['Q', 'S', 'E', 'R', 'D', 'A']

        m = self.record_tag.match(rec)
        if m:
            if m.group('type') in allowed_types:

                d = self.job_info_res.get(m.group('id'))
                if d is None:
                    d = {}

                d['status_type'] = m.group('type')
                if d['status_type'] != 'A':  # workaround 'A' poor format MM/DD/YYYY HH:mm:SS;A;job_id;Job rejected by all possible destinations
                    for a in m.group('msg').split():
                        (k, v) = a.split('=', 1)
                        if self.banned_columns.get(k):
                            continue
                        if k in self.int_keys:
                            v = int(v)
                        if k in self.walltime_to_seconds_keys:
                            hours, minutes, seconds = map(int, v.split(':'))
                            v = hours * 3600 + minutes * 60 + seconds

                        d[k] = v

                if m.group('type') == 'D':
                    d['end'] = self.logutils.convert_date_time(m.group('date') + ' ' + m.group('time'))
                    # pprint(d)  # DEBUG

                no_domain_id = m.group('id').split(sep='.')[0]
                self.job_info_res[no_domain_id] = d

        return PARSER_OK_CONTINUE

