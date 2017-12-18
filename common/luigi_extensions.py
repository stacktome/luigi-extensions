import importlib
import json
import logging
from itertools import chain

import luigi
from luigi.contrib.bigquery import BigqueryRunQueryTask, BigqueryTarget, BigqueryClient, BQTable


def accepts_exception(cls):
    attr_name = 'exception'
    method_name = '__init__'
    setattr(cls, attr_name, luigi.Parameter(default=''))
    cls_init = getattr(cls, method_name)

    def new_init(self, *args, **kwargs):
        cls_init(self, *args, **kwargs)
        exception = getattr(self, attr_name)
        if exception:
            raise Exception(exception)

    setattr(cls, method_name, new_init)
    return cls


def dynamic_dependencies_available(cls):
    """
        Usage in decorated luigi tasks:
            --tasks-requires '[{"name":"task_name", "module": "filename", "kwargs":{"date":"self.date"}}]
    """

    attr_name = 'tasks_requires'
    method_name = 'requires'
    setattr(cls, attr_name, luigi.Parameter(default=''))
    cls_requires = getattr(cls, method_name)

    def dynamic_requires(self):
        tasks = getattr(self, attr_name)
        if tasks:
            for task in json.loads(tasks):
                kwargs = {}
                for kwarg_key, kwarg_value in task['kwargs'].items():
                    if isinstance(kwarg_value, str) and kwarg_value.startswith('self.'):
                        kwargs[kwarg_key] = getattr(self, kwarg_value[5:])
                    else:
                        kwargs[kwarg_key] = kwarg_value
                yield getattr(importlib.import_module(task['module'], 'tasks'), task['name'])(**kwargs)

    def new_requires(self):
        if getattr(self, attr_name):
            return chain(cls_requires(self), dynamic_requires(self))
        else:
            return cls_requires(self)

    setattr(cls, method_name, new_requires)
    return cls


logger = logging.getLogger('luigi-interface')


class BigqueryRunQueryExtTask(BigqueryRunQueryTask):

    @property
    def write_disposition(self):
        """What to do if the table already exists. By default this will fail the job.
           See :py:class:`WriteDisposition`"""
        return 'WRITE_APPEND'

    @property
    def create_disposition(self):
        """Whether to create the table or not. See :py:class:`CreateDisposition`"""
        return 'CREATE_IF_NEEDED'

    @property
    def inlineUDF(self):
        """
        UDF code in Javascript, which is associated with particular BQ query/table.
        Note: if this is set, it will be preferred over udfURI property
        """
        return None

    @property
    def udfURI(self):
        """
        An array of URIs which point to the relevant UDF resources (e.g., Javascript files in Google Cloud)
        E.g.: [gs://bucket/my-udf.js]
        Note: if this is NOT set, inlineUDF property will be used
        """
        return None


    @property
    def useLegacySQL(self):
        """
        Defines whether :self.query is written using legacy SQL, or standard SQL (in newer versions of BQ).
        Return True, if legacy SQL syntax is used for query definition; False, if standard SQL syntax is used
        """
        return True

    def run(self):

        query = self.query
        assert query, 'No query was provided'
        tbl = BQTable(project_id=self.project_id, dataset_id=self.dataset, table_id=self.table)

        bq_client = BigqueryClient()
        logger.info('Launching Query')
        logger.info('Query destination: %s (%s)', self.table, self.write_disposition)
        logger.info('Query SQL: %s', query)

        udfResource = []
        if self.inlineUDF:
            udfResource.append({ 'inlineCode': self.inlineUDF})
        else:
            if self.udfURI:
                udfResource.append({ 'resourceUri': self.udfURI})
            else:
                #raise Exception("Either inlineUDF, or udfURI property must be set")
                pass

        job = {
            'projectId': tbl.project_id,
            'configuration': {
                'query': {
                    'query': query,
                    'priority': self.query_mode,
                    'destinationTable': {
                        'projectId': tbl.project_id,
                        'datasetId': tbl.dataset_id,
                        'tableId': tbl.table_id,
                    },
                    'allowLargeResults': True,
                    'createDisposition': self.create_disposition,
                    'writeDisposition': self.write_disposition,
                    'flattenResults': self.flatten_results,
                    'useQueryCache': False,
                    'useLegacySql': self.useLegacySQL,
                    'userDefinedFunctionResources': udfResource
                }
            }
        }
        bq_client.run_job(project_id=tbl.project_id, body=job, dataset=tbl.dataset)

        if not isinstance(self.output(), BigqueryTarget):
            self.output().touch()
