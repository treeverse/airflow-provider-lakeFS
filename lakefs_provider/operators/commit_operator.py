from typing import Any, Dict

from airflow.utils.decorators import apply_defaults
from airflow.utils.helpers import build_airflow_url_with_query

from lakefs_provider.hooks.lakefs_hook import LakeFSHook
from lakefs_provider.operators.with_metadata_operator import WithLakeFSMetadataOperator


class LakeFSCommitOperator(WithLakeFSMetadataOperator):
    """
    Commit changes to a lakeFS branch.

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo for the commit.
    :type repo: str
    :param branch: The branch name to commit.
    :type branch: str
    :param msg: The commit message.
    :type msg: str
    :param metadata: Additional metadata to the commit.
    :type metadata: Dict[str, str]
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
        'msg',
        'metadata'
    ]
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, msg: str, metadata: Dict[str, str] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch
        self.msg = msg
        self.metadata = metadata
        self.task_id = kwargs.get("task_id")

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info("Committing to lakeFS branch '%s' in repo '%s'",
                      self.branch, self.repo)

        self.metadata["airflow_task_id"] = self.task_id

        self.enrich_metadata(context)

        ref = hook.commit(self.repo, self.branch, self.msg, self.metadata)

        return ref
