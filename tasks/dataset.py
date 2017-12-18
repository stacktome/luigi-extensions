import luigi

import common.bq_dataset as bq_dataset


class CreateDataset(luigi.Task):
    """
        Usage:
            python -m luigi --module dataset DatasetChecker --datasets trust,test,test3 --project_id some_project_id
    """
    datasets = luigi.Parameter()
    project_id = luigi.Parameter(default='some_project_id')
    location = luigi.Parameter(default='EU')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.expected_datasets_names = str(self.datasets).split(',')

    def run(self):
        existing_datasets_names = bq_dataset.get_existing_datasets_names(self.project_id)
        new_datasets = self._get_nonexistent_datasets_names(self.expected_datasets_names,
                                                            existing_datasets_names)
        self._create_datasets(new_datasets)

    def output(self):
        return [bq_dataset.DatasetTarget(dataset_name, self.project_id)
                for dataset_name in self.expected_datasets_names]

    def _get_nonexistent_datasets_names(self, expected_datasets_names,
                                        existing_datasets_names):
        new_datasets = set()

        for expected_dataset in expected_datasets_names:
            if expected_dataset not in existing_datasets_names:
                new_datasets.add(expected_dataset)

        return new_datasets

    def _create_datasets(self, datasets):
        for dataset in datasets:
            bq_dataset.create_dataset(self.project_id, dataset,
                                      self.location)
