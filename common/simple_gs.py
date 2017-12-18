from google.cloud import storage

PROJECT = 'project_name'
BUCKET = 'bucket_name'


def get_gs_client():
    return storage.Client(project=PROJECT)


def get_gs_bucket(bucket_name):
    return get_gs_client().get_bucket(bucket_name)


def get_gs_blob(path, bucket_name):
    path = path[5:]  # remove 'gs://'
    rel_path = '/'.join(path.split('/')[1:])  # remove bucket name

    return get_gs_bucket(bucket_name).get_blob(rel_path)


class SimpleGSTarget(object):

    def __init__(self, path, limit=1024, project_id=PROJECT, bucket_name=BUCKET):
        client = storage.Client(project=project_id)
        bucket = client.get_bucket(bucket_name)

        self.limit = limit
        self.file = storage.Blob(path, bucket)

    def exists(self):
        try:
            self.file.reload()
            return self.file.exists() and self.file.size/1048576 < self.limit # MiB
        except:
            return False

    def delete(self):
        try:
            self.file.delete()
            self.file.reload()
            return not self.file.exists()
        except:
            return not self.file.exists()


def gs_path_builder(bucket_name, date, filename):
    pattern = 'gs://{bucket}/{year}/{month}/{day}/{filename}'
    month, day = '{0:02d}'.format(date.month), '{0:02d}'.format(date.day)

    full_path = pattern.format(bucket=bucket_name, year=date.year, month=month, day=day, filename=filename)
    relative_path = full_path[full_path.index(bucket_name)+len(bucket_name)+1:]

    return {'full_path': full_path, 'relative_path': relative_path, 'filename': filename}
