from hera.task import Task
from hera.workflow import Workflow
from hera.workflow_service import WorkflowService
from hera.artifact import InputArtifact, OutputArtifact

TOKEN="eyJhbGciOiJSUzI1NiIsImtpZCI6IlhiQzVoeGlqNTlkTFpUWnlEbVhKM01EMGFZbzltZFk4LWhZZ0dfZzdaUWsifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJhcmdvIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImhlcmEtdG9rZW4tc2I0ajkiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiaGVyYSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjI0Y2YzYjRhLTE0YTYtNDRlOS04NjJkLTlkMjdmZTQ2YTQ0ZCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDphcmdvOmhlcmEifQ.Cj5otDy8VV9fs900nGpDfx_XJbVjH6ct3zU94mDEKTjdfDwVxxjaYALpbisz-tTXM9dn6jM41l-R_5vQjiGuLWXDPLcA2cs1eIR0qIaWrq_mBbnSOlpNFiNJ3l9dwiDykVE753jmtu0mM2nhQoZiK0xLdIZtUBOePGR4WeNg-vkt9gg87LfMMotE-8XS-yyz6MjAxu1bqO5Mkh-Zoh2Vg8kKXBMEkT7-M8PSgQP-J64aaDZCOeEdQHow1KzsyZTGv33Xw6xQLRBrma18q7UE5BT2NtbOFBiRP7i5Yb3NBUMGhPfDDPLkqDPNzyGa2BjQFrG-JLWGalw1Fdg3PDNpXg"
# Pipeline step to generate faker data.

def generate_faker_data():
    from faker import Faker
    import numpy as np
    import pandas as pd
    

    fake = Faker()

    profs = []
    for _ in range(100):
        profs.append(fake.profile())

    feature1 = np.random.rand(100)
    feature2 = np.random.rand(100)

    data = pd.DataFrame(profs)
    data['feature1'] = feature1
    data['feature2'] = feature2

    data.to_csv("/data/faker_test.csv", index=False)


def validate_data():
    from minio import Minio
    import tarfile
    import pandas as pd
    from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
    from great_expectations.data_context import BaseDataContext
    import os

    client = Minio(
        "ge-minio:9000",
        access_key="admin",
        secret_key="KcYqiHA1Iw",
        secure=False
    )

    client.fget_object(
        "great-expectations", "ge-store.tar.gz", "./ge-store.tar.gz",
    )

    file = tarfile.open('ge-store.tar.gz')
    file.extractall('/ge-store/')
    file.close()

    data_context_config = DataContextConfig(
        datasources={
            "pandas": DatasourceConfig(
                class_name="Datasource",
                execution_engine={
                    "class_name": "PandasExecutionEngine"
                },
                data_connectors={
                    "faker_data": {
                        "class_name": "ConfiguredAssetFilesystemDataConnector",
                        "base_directory": "/data",
                        "assets": {
                            "faker_data": {
                                "pattern": r"(.*)",
                                "group_names": ["data_asset"]
                            }
                        },
                    }
                },
            )
        },
        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/ge-store/ge-store"),
    )

    context = BaseDataContext(project_config=data_context_config)

    datasource_config = {
        "name": "faker_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "/data/",
                "default_regex": {"group_names": ["data_asset_name"], "pattern": "(.*)"},
            },
        },
    }

    context.add_datasource(**datasource_config)

    checkpoint_config = {
        "name": "my_first_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "faker_data",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": "test",
                },
                "expectation_suite_name": "test_suite",
            }
        ],
    }

    context.add_checkpoint(**checkpoint_config)

    df = pd.read_csv("/data/faker_test.csv")

    results = context.run_checkpoint(
        checkpoint_name="my_first_checkpoint",
        batch_request={
            "runtime_parameters": {"batch_data": df},
            "batch_identifiers": {
                "default_identifier_name": "first_batch_test"
            },
        },
    )

    with tarfile.open("ge-results.tar.gz", "w:gz") as tar:
        tar.add("/ge-store/ge-store", arcname=os.path.basename("/ge-store/ge-store/"))
    
    client.fput_object(
        "great-expectations", "ge-results.tar.gz", "./ge-results.tar.gz",
    )


ws = WorkflowService(host="https://localhost:2746", verify_ssl=False, token=TOKEN)
w = Workflow("generate-expectations", ws, namespace="argo")

faker_task = Task("faker-data", generate_faker_data, image="lambertsbennett/argo-ge:v1", 
                    output_artifacts = [OutputArtifact(name="FakerData", path="/data/faker_test.csv")])

ge_task = Task("great-expectations-val", validate_data, image="lambertsbennett/argo-ge:v1", 
                    input_artifacts = [InputArtifact(name="FakerData", path="/data/faker_test.csv", from_task="faker-data", artifact_name="FakerData")])

faker_task >> ge_task

w.add_tasks(faker_task, ge_task)
w.submit()