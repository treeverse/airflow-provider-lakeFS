from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakefsHook


class CommitSensor(BaseSensorOperator):
    """
    Executes a get branch operation until that branch was committed.

    :param lakefs_conn_id: The connection to run the sensor against
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo.
    :type repo: str
    :param branch: The branch to sense for
    :type branch: str
    :param branch_exists: if true, 404 responses will be considered as errors
    :type branch_exists: bool
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'branch',
        'branch_exists',
    ]

    current_commit_id_key = 'current_commit_id'
    branch_not_found_error = "Resource Not Found"

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, branch: str, branch_exists: bool = True, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch

        # Branch may not exists when this sensor was created and that's ok
        self.branch_exists = branch_exists
        self.hook = LakefsHook(lakefs_conn_id)
        self.prev_commit_id = None

    def poke(self, context: Dict[Any, Any]) -> bool:
        if self.prev_commit_id is None:
            self.prev_commit_id = context.get(self.current_commit_id_key, None)
            if self.prev_commit_id is None:
                self.prev_commit_id = self.hook.get_branch_commit_id(self.repo, self.branch)

        self.log.info('Poking: branch %s on repo %s', self.branch, self.repo)
        try:
            curr_commit_id = self.hook.get_branch_commit_id(self.repo, self.branch)
            self.branch_exists = True

            self.log.info('Previous ref: %s, current ref %s', self.prev_commit_id, curr_commit_id)
            return curr_commit_id != self.prev_commit_id

        except AirflowException as exc:
            if (not self.branch_exists) and self.branch_not_found_error in str(exc):
                return False

        return False
