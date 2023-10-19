from typing import Any, Dict, IO, Iterator

from lakefs_provider import __version__

import lakefs_sdk
from lakefs_sdk import models
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.models.object_stats import ObjectStats
from lakefs_sdk.models import Merge

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class LakeFSHook(BaseHook):
    """
    LakeFSHook that interacts with a lakeFS server.

    :param lakefs_conn_id: connection that has the uses the extra fields to extract the
        access_key_id, secret_access_key and lakeFS server endpoint.
    :type lakefs_conn_id: str
    """
    conn_name_attr = "lakefs_conn_id"
    client_id = f"lakefs-airflow-provider/{__version__}"
    default_conn_name = "lakefs_default"
    conn_type = "lakefs"
    hook_name = "lakeFS"

    def __init__(self, lakefs_conn_id: str) -> None:
        super().__init__()
        self.lakefs_conn_id = lakefs_conn_id

    def get_base_url(self) -> str:
        base = self.get_connection(self.lakefs_conn_id).host
        if not (base.startswith('http://') or base.startswith('https://')):
            base = f"http://{base}"
        return base

    def get_conn(self) -> LakeFSClient:
        conn = self.get_connection(self.lakefs_conn_id)
        configuration = lakefs_sdk.Configuration()
        if conn.conn_type == "http" and conn.extra_dejson.get("access_key_id") and conn.extra_dejson.get(
                "secret_access_key"):
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

        return LakeFSClient(configuration,
                            header_name='X-Lakefs-Client', header_value=self.client_id)

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "description", "port", "extra"],
            "relabeling": {"host": "lakeFS URL", "login": "lakeFS access key", "password": "lakeFS secret key"},
            "placeholders": {},
        }

    def create_branch(self, repository: str, name: str, source_branch: str = 'main') -> str:
        client = self.get_conn()
        ref = client.branches_api.create_branch(
            repository=repository, branch_creation=models.BranchCreation(name=name,
                                                                         source=source_branch))
        return ref

    def commit(self, repo: str, branch: str, msg: str, metadata: Dict[str, Any] = None) -> str:
        client = self.get_conn()
        commit = client.commits_api.commit(
            repository=repo,
            branch=branch,
            commit_creation=models.CommitCreation(message=msg, metadata=metadata))

        return commit.id

    def upload(self, repo: str, branch: str, path: str, content: bytes) -> str:
        client = self.get_conn()
        upload = client.objects_api.upload_object(
            repository=repo,
            branch=branch,
            path=path,
            content=content)

        return upload.physical_address

    def merge(self, repo: str, source_ref: str, destination_branch: str,
              msg: str, metadata: Dict[str, Any] = None) -> str:
        client = self.get_conn()
        merge_result = client.refs_api.merge_into_branch(
            repository=repo,
            source_ref=source_ref,
            destination_branch=destination_branch,
            merge=Merge(message=msg, metadata=metadata))

        return merge_result.reference

    def get_branch_commit_id(self, repo: str, name: str) -> str:
        client = self.get_conn()
        ref = client.branches_api.get_branch(repo, name)
        return ref.commit_id

    def get_commit(self, repo: str, ref: str) -> Dict[str, str]:
        client = self.get_conn()
        commit = client.commits_api.get_commit(repo, ref)
        return commit.to_dict()

    def log_commits(self, repo: str, ref: str, size: int = 100) -> Iterator:
        """Yield commits of repo backwards from ref.
        Fetch size commits at a time."""
        client = self.get_conn()
        after = ''
        while True:
            response = client.refs_api.log_commits(repo, ref, amount=size, after=after)
            for details in response.results:
                yield details.to_dict()
            if response.pagination is None or not response.pagination.has_more:
                return
            after = response.pagination.next_offset

    def stat_object(self, repo: str, ref: str, path: str) -> ObjectStats:
        client = self.get_conn()
        response = client.objects_api.stat_object(repository=repo, ref=ref, path=path)
        return response.to_dict()

    def get_object(self, repo: str, ref: str, path: str) -> IO:
        client = self.get_conn()
        return client.objects_api.get_object(repository=repo, ref=ref, path=path)

    def create_symlink_file(self, repo: str, branch: str, location: str = None) -> str:
        client = self.get_conn()

        kwargs = {}
        if location:
            kwargs["location"] = location

        response = client.internal_api.create_symlink_file(repository=repo, branch=branch, **kwargs)
        return response.location

    def delete_branch(self, repo: str, branch: str) -> str:
        client = self.get_conn()
        return client.branches_api.delete_branch(repository=repo, branch=branch)

    def test_connection(self):
        """Test Connection"""
        conn = self.get_connection(self.lakefs_conn_id)
        import requests
        import json
        url = conn.host + "/api/v1/auth/login"
        if conn.conn_type == "http" and conn.extra_dejson.get("access_key_id") and conn.extra_dejson.get(
                "secret_access_key"):
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
            return True, "Connection Tested Successfully"
        except requests.exceptions.URLRequired as e:
            return False, str(e)
        except requests.exceptions.HTTPError as e:
            return False, str(e)
        except requests.exceptions.RequestException as e:
            return False, str(e)
