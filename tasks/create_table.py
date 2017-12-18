import json
from datetime import datetime
from time import sleep

import luigi
from luigi.contrib.bigquery import BigqueryTarget, BigqueryClient

from common.gbq import GbqConnector
from common.luigi_extensions import accepts_exception
from common.postgres import default_postgres_target
from common.simple_gs import get_gs_blob
from common.utils import run_once

from tasks.dataset import CreateDataset


class CreateTableParams:
    table = luigi.Parameter()
    dataset = luigi.Parameter()
    project_id = luigi.Parameter(default='default_project_name')
    schema = luigi.Parameter(default='')
    is_partitioned = luigi.BoolParameter(default=False)


class CreateAndOrLoadTableParams(CreateTableParams):
    gs_path = luigi.Parameter(default='')
    bucket_name = luigi.Parameter(default='default_bucket_name')
    write_disposition = luigi.Parameter(default='WRITE_APPEND')
    create_disposition = luigi.Parameter(default='CREATE_IF_NEEDED')
    date = luigi.DateParameter(default=datetime.now().date())
    end_date = luigi.Parameter('')
    force = luigi.BoolParameter(default=False)
    csv_null_marker = luigi.Parameter(default='')
    account_name = luigi.Parameter(default='')
    delimiter = luigi.Parameter(default='\t')
    skip_leading_rows = luigi.IntParameter(default=0)
    max_bad_records = luigi.IntParameter(default=0)
    parent_task = luigi.Parameter(default='')


@accepts_exception
class CreateAndOrLoadTable(luigi.ExternalTask, CreateAndOrLoadTableParams):
    @run_once
    def requires(self):
        yield CreateTable(schema=self.schema, table=self.table, dataset=self.dataset, project_id=self.project_id)

    def run(self):
        gs_file = get_gs_blob(self.gs_path, self.bucket_name)

        # gs_file.reload()
        if gs_file.exists():
            if gs_file.size > 0:
                self._create_table_from_data()

            self.output().touch()
        else:
            raise Exception('Gs file doesn\'t exists, path=%s' % self.gs_path)

    @run_once
    def output(self):
        other_params = [self.dataset, self.table, self.account_name]

        if self.end_date and self.parent_task and self.account_name:
            other_params.insert(0, self.parent_task)
            other_params.append(str(self.end_date))
            other_params.append(self.account_name)
        else:
            other_params.insert(0, 'Dataload')

        return default_postgres_target(
            self, use_default_pattern=True, date=self.date, other_params=other_params)

    def _create_table_from_data(self):
        jobs = GbqConnector(project_id=self.project_id).service.jobs()
        file_extension = str(self.gs_path).split('.')[-1]

        job = {
            'projectId': self.project_id,
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': self.project_id,
                        'datasetId': self.dataset,
                        'tableId': self.table,
                    },
                    'sourceUris': [self.gs_path],
                    'createDisposition': self.create_disposition,
                    'writeDisposition': self.write_disposition,
                    'maxBadRecords': self.max_bad_records
                }
            }
        }

        if not self.schema:
            job['configuration']['load']['autodetect'] = True

        if file_extension == 'json':
            job['configuration']['load']['sourceFormat'] = 'NEWLINE_DELIMITED_JSON'
        elif file_extension == 'csv':
            job['configuration']['load']['sourceFormat'] = 'CSV'
            job['configuration']['load']['fieldDelimiter'] = self.delimiter
            job['configuration']['load']['skipLeadingRows'] = self.skip_leading_rows
            job['configuration']['load']['nullMarker'] = self.csv_null_marker

        if self.write_disposition == 'WRITE_APPEND':
            job['configuration']['load']['schemaUpdateOptions'] = ['ALLOW_FIELD_ADDITION']

        response = jobs.insert(projectId=self.project_id, body=job).execute()
        job_id = response['jobReference']['jobId']

        result = self._poll_job(jobs, job_id)

        if result == 'Autodetect fails':
            del job['configuration']['load']['autodetect']
            del job['configuration']['load']['schemaUpdateOptions']
            response = jobs.insert(projectId=self.project_id, body=job).execute()
            job_id = response['jobReference']['jobId']
            self._poll_job(jobs, job_id)

    def _poll_job(self, jobs, job_id):
        while True:
            status = jobs.get(projectId=self.project_id, jobId=job_id).execute()['status']

            if 'errorResult' in status:
                if str(status['errorResult']['message']).startswith('Invalid schema update'):
                    return 'Autodetect fails'
                else:
                    raise Exception(str(status))

            if status['state'] == 'DONE':
                return 'DONE'

            sleep(1)


class CreateTable(luigi.Task, CreateTableParams):
    """
    Example:
        python -m luigi --module create_table CreateTable --dataset test --table test_table --schema schemas/schema.json
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client = BigqueryClient()

    def requires(self):
        return CreateDataset(datasets=self.dataset, project_id=self.project_id)

    def run(self):
        self._create_table()

    def complete(self):
        return self.output().exists()

    def output(self):
        return BigqueryTarget(self.project_id, self.dataset, self.table)

    def _create_table(self):
        dataset_ref = {'datasetId': self.dataset,
                       'projectId': self.project_id}
        table_ref = {'tableId': self.table,
                     'datasetId': self.dataset,
                     'projectId': self.project_id}
        table = {'tableReference': table_ref}

        if self.schema:
            with open(self.schema, 'r') as f:
                schema = json.load(f)

            table['schema'] = {'fields': schema} if 'fields' not in schema else schema

        if self.is_partitioned:
            table['timePartitioning'] = {
                'type': 'DAY'
            }

        self.client.client.tables() \
            .insert(body=table, **dataset_ref) \
            .execute()


class TableSchema(luigi.ExternalTask):
    dataset = luigi.Parameter()
    table = luigi.Parameter()
    schemas_path = luigi.Parameter()

    @property
    def schema_path(self):
        return '{root}/{dataset}.{table}.json'.format(root=self.schemas_path, dataset=self.dataset, table=self.table)

    def output(self):
        return luigi.LocalTarget(self.schema_path)
