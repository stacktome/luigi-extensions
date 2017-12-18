from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.cloud import bigquery
import luigi


class DatasetTarget(luigi.Target):
    def __init__(self, dataset_name, project_id):
        self.dataset_name = dataset_name
        self.project_id = project_id

    def exists(self):
        if self.dataset_name not in get_existing_datasets_names(self.project_id):
            return False

        return True


def get_existing_datasets(project_id):
    client = bigquery.Client(project=project_id)
    return client.list_datasets()


def get_existing_datasets_names(project_id):
    datasets = get_existing_datasets(project_id)
    datasets_names = []

    for dataset in datasets:
        datasets_names.append(dataset.name)

    return datasets_names


def get_service():
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('bigquery', 'v2', credentials=credentials)


def create_dataset(project_id, dataset_name, location='EU'):
    service = get_service()

    dataset = {
        "datasetReference": {
            "datasetId": dataset_name,
            "projectId": project_id
        },
        "location": location
    }

    dataset_collection = service.datasets()
    dataset_collection.insert(projectId=project_id, body=dataset).execute()


def get_table_info(project_id, dataset_id, table_id):
    service = get_service()

    table = service.tables().get(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id
    ).execute()

    return table


def insert_job(project_id, dataset, table, query):
    service = get_service()

    response = {
        "configuration": {
            "query": {
                "query": query,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset,
                    "tableId": table
                },
                "writeDisposition": "WRITE_APPEND",
                "useLegacySql": False
            }
        }
    }

    jobs_collection = service.jobs()
    response = jobs_collection.insert(projectId=project_id, body=response).execute()

    if 'errorResult' in response['status']:
        raise Exception(response['status']['errorResult']['message'])
