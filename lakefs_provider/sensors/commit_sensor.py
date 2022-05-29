from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from lakefs_client.exceptions import NotFoundException

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSCommitSensor(BaseSensorOperator):
    """
    Executes a get branch operation until that branch was committed.

    :param lakefs_conn_id: The connection to run the sensor against
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo.
    :type repo: str
    :param branch: The branch to sense for.
    :type branch: str
    :param prev_commit_id: If present, previous last commit ID on branch; wait until it changes.
    :type prev_commit_id: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
        'prev_commit_id',
    ]

    current_commit_id_key = 'current_commit_id'
    branch_not_found_error = "Resource Not Found"

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, prev_commit_id: str = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch
        self.prev_commit_id = prev_commit_id

        self.hook = LakeFSHook(lakefs_conn_id)

    def poke(self, context: Dict[Any, Any]) -> bool:
        if self.prev_commit_id is None:
            self.prev_commit_id = context.get(self.current_commit_id_key, None)
            if self.prev_commit_id is None:
                self.prev_commit_id, _ = self.get_commit()
                return False

        self.log.info('Poking: branch %s on repo %s', self.branch, self.repo)
        curr_commit_id, exists = self.get_commit()
        if not exists:
            return False

        self.log.info('Previous ref: %s, current ref %s', self.prev_commit_id, curr_commit_id)
        return curr_commit_id != self.prev_commit_id

    def get_commit(self) -> (str, bool):
        try:
            commit_id = self.hook.get_branch_commit_id(self.repo, self.branch)
        except NotFoundException:
            self.log.info("Branch '%s' not found in repo '%s'", self.branch, self.repo)
            return None, False

        return commit_id, True


