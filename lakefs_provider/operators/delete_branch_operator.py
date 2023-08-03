from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSDeleteBranchOperator(BaseOperator):
    """
    Delete a lakeFS branch by calling the lakeFS server.

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo where the branch is deleted.
    :type repo: str
    :param branch: The branch name to delete
    :type branch: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
    ]
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info(f"Delete lakeFS branch {self.branch} in repo {self.repo}")
        return hook.delete_branch(self.repo, self.branch)
