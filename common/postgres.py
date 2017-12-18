from datetime import datetime

import luigi
from luigi.postgres import PostgresTarget

TODAY = datetime.now().date()


class ExtendedPostgresTarget(PostgresTarget):
    def remove(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True

        connection.cursor().execute(
            """DELETE FROM {marker_table} WHERE update_id = %s""".format(
                marker_table=self.marker_table), (self.update_id,))

        assert not self.exists(connection)


def get_postgres_configs():
    try:
        from configparser import NoSectionError
    except ImportError:
        from ConfigParser import NoSectionError  # Python < 3.0

    config = luigi.configuration.get_config()
    return (config.get('postgres', 'database'),
            config.get('postgres', 'username'),
            config.get('postgres', 'password'),
            '{0}:{1}'.format(config.get('postgres', 'host'), config.get('postgres', 'port')))


DATABASE, USER, PASSWORD, HOST = get_postgres_configs()


def default_postgres_target(self, host=HOST, database=DATABASE, user=USER, password=PASSWORD, date=TODAY,
                            other_params=None, use_default_pattern=True):
    update_id = ''
    self_values = self.__dict__
    task_name = type(self).__name__

    if use_default_pattern:
        pattern = '{date}_{task_name}_{project_id}'
        update_id += pattern.format(date=date, task_name=task_name,
                                    project_id=self_values['param_kwargs']['project_id'])

    if other_params:
        for update_info in other_params:
            if update_id:
                update_id += '_' + str(update_info)
            else:
                update_id += str(update_info)

    table = self_values.get('table', '')
    if not table:
        table += self_values['param_kwargs'].get('table', 'unknown_table')

    dataset = self_values.get('dataset', '')
    if not dataset:
        dataset += self_values['param_kwargs'].get('dataset', 'unknown_dataset')
    update_id = '{0}.{1}_'.format(dataset, table) + update_id
    target = ExtendedPostgresTarget(host=host, database=database, user=user, password=password,
                                    table='{0}.{1}'.format(dataset, table),
                                    update_id=update_id)

    if self_values['param_kwargs'].get('force', False) and target.exists():
        target.remove()

    return target
