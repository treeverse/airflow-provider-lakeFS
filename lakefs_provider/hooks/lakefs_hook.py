import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class LakefsHook(BaseHook):
    """
    LakefsHook that interacts with a lakeFS server.

    :param lakefs_conn_id: connection that has the uses the extra fields to extract the
        access_key_id, secret_access_key and lakeFS server endpoint.
    """

    def __init__(self, lakefs_conn_id: str) -> None:
        super().__init__()
        self.lakefs_conn_id = lakefs_conn_id

    def get_conn(self) -> LakeFSClient:
        conn = self.get_connection(self.lakefs_conn_id)

        configuration = lakefs_client.Configuration()
        configuration.username = conn.extra_dejson.get('access_key_id', None)
        configuration.password = conn.extra_dejson.get('secret_access_key', None)
        configuration.host = conn.extra_dejson.get('endpoint', None)

        if configuration.username is None:
            raise AirflowException("access_key_id must be specified in the lakeFS connection details")
        if configuration.password is None:
            raise AirflowException("secret_access_key must be specified in the lakeFS connection details")
        if configuration.host is None:
            raise AirflowException("endpoint must be specified in the lakeFS connection details")

        return LakeFSClient(configuration)

    def create_branch(self, repo: str, name: str, source_branch: str = 'main') -> str:
        client = self.get_conn()
        try:
            ref = client.branches.create_branch(
                repository=repo, branch_creation=models.BranchCreation(name=name,
                                                                       source=source_branch))
        except Exception as exc:
            raise AirflowException("Failed to create a branch") from exc

        return ref

    def get_branch_commit_id(self, repo: str, name: str) -> str:
        client = self.get_conn()
        try:
            ref = client.branches.get_branch(repo, name)
        except Exception as exc:
            raise AirflowException("Failed to create a branch") from exc

        return ref.commit_id
