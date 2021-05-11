from typing import Dict, Any

import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient
from lakefs_client.model.object_stats import ObjectStats
from lakefs_client.models import Merge

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class LakeFSHook(BaseHook):
    """
    LakeFSHook that interacts with a lakeFS server.

    :param lakefs_conn_id: connection that has the uses the extra fields to extract the
        access_key_id, secret_access_key and lakeFS server endpoint.
    """

    def __init__(self, lakefs_conn_id: str) -> None:
        super().__init__()
        self.lakefs_conn_id = lakefs_conn_id

    def get_conn(self) -> LakeFSClient:
        conn = self.get_connection(self.lakefs_conn_id)

        configuration = lakefs_client.Configuration()
        configuration.username = conn.extra_dejson.get("access_key_id", None)
        configuration.password = conn.extra_dejson.get("secret_access_key", None)
        configuration.host = conn.host

        if not configuration.username:
            raise AirflowException("access_key_id must be specified in the lakeFS connection details")
        if not configuration.password:
            raise AirflowException("secret_access_key must be specified in the lakeFS connection details")
        if not configuration.host:
            raise AirflowException("lakeFS endpoint must be specified in the lakeFS connection details")

        return LakeFSClient(configuration)

    def create_branch(self, repository: str, name: str, source_branch: str = 'main') -> str:
        client = self.get_conn()
        ref = client.branches.create_branch(
            repository=repository, branch_creation=models.BranchCreation(name=name,
                                                                         source=source_branch))

        return ref

    def commit(self, repo: str, branch: str, msg: str, metadata: Dict[str, Any] = None) -> str:
        client = self.get_conn()
        commit = client.commits.commit(
            repository=repo,
            branch=branch,
            commit_creation=models.CommitCreation(message=msg, metadata=metadata))

        return commit.get("id")

    def merge(self, repo: str, source_ref: str, destination_branch: str,
              msg: str, metadata: Dict[str, Any] = None) -> str:
        client = self.get_conn()
        merge_result = client.refs.merge_into_branch(
            repository=repo,
            source_ref=source_ref,
            destination_branch=destination_branch,
            merge=Merge(message=msg, metadata=metadata))

        return merge_result.get("reference")

    def get_branch_commit_id(self, repo: str, name: str) -> str:
        client = self.get_conn()
        ref = client.branches.get_branch(repo, name)
        return ref.commit_id

    def stat_object(self, repo: str, ref: str, path: str) -> ObjectStats:
        client = self.get_conn()
        return client.objects.stat_object(repository=repo, ref=ref, path=path)


