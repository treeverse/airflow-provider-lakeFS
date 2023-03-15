from typing import Any, Dict, IO, Iterator

import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient
from lakefs_client.model.object_stats import ObjectStats
from lakefs_client.models import Merge

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


def _commitAsDict(details):
    return {
        "id": details.id,
        "parents": details.parents,
        "committer": details.committer,
        "message": details.message,
        "creation_date": details.creation_date,
        "meta_range_id": details.meta_range_id,
        "metadata": details.metadata,
    }


class LakeFSHook(BaseHook):
    """
    LakeFSHook that interacts with a lakeFS server.

    :param lakefs_conn_id: connection that has the uses the extra fields to extract the
        access_key_id, secret_access_key and lakeFS server endpoint.
    :type lakefs_conn_id: str
    """
    conn_name_attr = "lakefs_conn_id"
    default_conn_name = "lakefs_default"
    conn_type = "lakefs"
    hook_name = "lakeFS"

    def __init__(self, lakefs_conn_id: str) -> None:
        super().__init__()
        self.lakefs_conn_id = lakefs_conn_id

    def get_conn(self) -> LakeFSClient:
        conn = self.get_connection(self.lakefs_conn_id)
        configuration = lakefs_client.Configuration()
        if conn.conn_type == "http" and conn.extra_dejson.get("access_key_id") and conn.extra_dejson.get("secret_access_key"):
            configuration.username = conn.extra_dejson.get("access_key_id")
            configuration.password = conn.extra_dejson.get("secret_access_key")
        else:
            configuration.username = conn.login
            configuration.password = conn.password
        configuration.host = conn.host
        if not configuration.username:
            raise AirflowException("access_key_id must be specified in the lakeFS connection details")
        if not configuration.password:
            raise AirflowException("secret_access_key must be specified in the lakeFS connection details")
        if not configuration.host:
            raise AirflowException("lakeFS endpoint must be specified in the lakeFS connection details")

        return LakeFSClient(configuration)

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "description","port","extra"],
            "relabeling": {"host": "lakeFS URL", "login": "lakeFS access key", "password":"lakeFS secret key"},
            "placeholders": {},
        }

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

    def upload(self, repo: str, branch: str, path: str, content: IO) -> str:
        client = self.get_conn()
        upload = client.objects.upload_object(
            repository=repo,
            branch=branch,
            path=path,
            content=content)

        return upload

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

    def get_commit(self, repo: str, ref: str) -> Dict[str, str]:
        client = self.get_conn()
        # Actually ask for a log of length 1, because of treeverse/lakeFS#3436.
        response = client.refs.log_commits(repo, ref, amount=1)
        details = response.results[0]
        return _commitAsDict(details)

    def log_commits(self, repo: str, ref: str, size: int=100) -> Iterator:
        """Yield commits of repo backwards from ref.

        Fetch size commits at a time."""
        client = self.get_conn()
        log_commits = client.refs.log_commits
        after = ''
        while True:
            response = log_commits(repo, ref, amount=size, after=after)
            for details in response.results:
                yield _commitAsDict(details)
            if response.pagination == None or not response.pagination.has_more:
                return
            after = response.pagination.next_offset

    def stat_object(self, repo: str, ref: str, path: str) -> ObjectStats:
        client = self.get_conn()
        return client.objects.stat_object(repository=repo, ref=ref, path=path)

    def get_object(self, repo: str, ref: str, path: str) -> IO:
        client = self.get_conn()
        return client.objects.get_object(repository=repo, ref=ref, path=path)

    def create_symlink_file(self, repo: str, branch: str, location: str = None)  -> str:
        client = self.get_conn()

        kwargs = {}
        if location:
            kwargs["location"] = location

        return client.metadata.create_symlink_file(repository=repo, branch=branch, **kwargs)["location"]

    def test_connection(self):
        """Test  Connection"""
        conn = self.get_connection(self.lakefs_conn_id)
        import requests
        import json
        url = conn.host+"/api/v1/auth/login"
        if conn.conn_type == "http" and conn.extra_dejson.get("access_key_id") and conn.extra_dejson.get("secret_access_key"):
            login = conn.extra_dejson.get("access_key_id")
            password = conn.extra_dejson.get("secret_access_key")
        else:
            login = conn.login
            password = conn.password

        payload = json.dumps({
            "access_key_id": login,
            "secret_access_key": password})
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload)
        try:
            response.raise_for_status()
            return True,"Connection Tested Successfully"
        except requests.exceptions.URLRequired as e:
            return  False,str(e)
        except requests.exceptions.HTTPError as e:
            return  False,str(e)
        except requests.exceptions.RequestException as e:
            return  False,str(e)
