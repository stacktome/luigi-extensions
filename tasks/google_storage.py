import os

import luigi
from luigi.contrib.gcs import GCSTarget, GCSClient

from common.luigi_extensions import accepts_exception
from common.utils import run_once


class UploadToGsParams:
    gs_path = luigi.Parameter()  # example: gs://datalake-sf/2017/01/04/test_data.gz
    mime_type = luigi.Parameter()
    file_path = luigi.Parameter(default='')
    string = luigi.Parameter(default='')
    remove_file = luigi.BoolParameter(default=False)
    force = luigi.BoolParameter(default=False)


@accepts_exception
class UploadToGs(luigi.ExternalTask, UploadToGsParams):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.force and self.output().exists():
            self.output().remove()

    @run_once
    def output(self):
        return GCSTarget(self.gs_path)

    def run(self):
        client = GCSClient()
        if self.string:
            client.put_string(contents=self.string, dest_path=self.gs_path, mimetype=self.mime_type)
        elif self.file_path:
            client.put(filename=self.file_path, dest_path=self.gs_path, mimetype=self.mime_type)
            if self.remove_file:
                os.remove(self.file_path)
