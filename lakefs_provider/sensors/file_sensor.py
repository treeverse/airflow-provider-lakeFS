from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from lakefs_client.exceptions import NotFoundException

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class FileSensor(BaseSensorOperator):
    """
    Waits for the given file to appear

    :param lakefs_conn_id: The connection to run the sensor against
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo.
    :type repo: str
    :param branch: The branch to sense for.
    :type branch: str
    :param path: The path to wait for.
    :type path: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
        'path',
    ]

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, path: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch
        self.path = path

        self.hook = LakeFSHook(lakefs_conn_id)

    def poke(self, context: Dict[Any, Any]) -> bool:
        try:
            self.hook.stat_object(self.repo, self.branch, self.path)
            self.log.info("Found file '%s' on branch '%s'", self.path, self.branch)
            return True

        except NotFoundException:
            self.log.info("File '%s' not found on branch '%s'", self.path, self.branch)
            return False

