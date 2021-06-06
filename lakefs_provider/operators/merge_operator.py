from typing import Any, Dict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSMergeOperator(BaseOperator):
    """
    Merge source branch to destination branch

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo.
    :type repo: str
    :param source_ref: The source reference to merge from.
    :type source_ref: str
    :param destination_branch: The destination branch to merge to.
    :type destination_branch: str
    :param msg: The commit message.
    :type msg: str
    :param metadata: The commit message.
    :type metadata: Additional metadata to the commit
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'source_ref',
        'destination_branch',
        'msg',
        'metadata'
    ]
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, source_ref: str, destination_branch: str, msg: str, metadata: Dict[str, str] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.source_ref = source_ref
        self.destination_branch = destination_branch
        self.msg = msg
        self.metadata = metadata
        self.task_id = kwargs.get("task_id")

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info("Merging to lakeFS branch '%s' in repo '%s' from source ref '%s'",
                      self.destination_branch, self.repo, self.source_ref)

        self.metadata.__setitem__("airflow_task_id", self.task_id)
        ref = hook.merge(self.repo, self.source_ref, self.destination_branch, self.msg, self.metadata)

        return ref
