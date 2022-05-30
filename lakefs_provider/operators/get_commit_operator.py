from typing import Any, Dict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSGetCommitOperator(BaseOperator):
    """
    Get commit details for a lakeFS ref.

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo for the commit.
    :type repo: str
    :param ref: The reference to fetch, can be branch, tag, digest, expression, etc.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'ref',
    ]
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, ref: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.ref = ref
        self.task_id = kwargs.get("task_id")

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info("Get commit for '%s' in repo '%s'",
                      self.ref, self.repo)

        details = hook.get_commit(self.repo, self.ref)

        return details
