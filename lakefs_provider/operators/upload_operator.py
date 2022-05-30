from typing import Any, Dict, IO

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSUploadOperator(BaseOperator):
    """
    Upload an object to a lakeFS repo.

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo for the desired object.
    :type repo: str
    :param branch: The branch name for the desired object.
    :type branch: str
    :param path: The path for the desired object.
    :type msg: str
    :param content: Contents of the desired object.
    :type content: typing.IO
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
        'path',
    ]
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, path: str, content: IO = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch
        self.path = path
        self.content = content
        self.task_id = kwargs.get("task_id")

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info("Uploading to path '%s' on lakeFS branch '%s' in repo '%s'",
                      self.path, self.branch, self.repo)

        stats = hook.upload(self.repo, self.branch, self.path, self.content)

        return stats.physical_address
