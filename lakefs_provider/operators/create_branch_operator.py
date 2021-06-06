from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSCreateBranchOperator(BaseOperator):
    """
    Create a lakeFS branch by calling the lakeFS server.

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo where the branch is created.
    :type repo: str
    :param branch: The branch name to create
    :type branch: str
    :param source_branch: The source branch to branch out from
    :type source_branch: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
        'source_branch',
    ]
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, source_branch: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch
        self.source_branch = source_branch

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info("Create lakeFS branch '%s' in repo '%s' from source '%s'",
                      self.branch, self.repo, self.source_branch)
        ref = hook.create_branch(self.repo, self.branch, self.source_branch)

        return ref
