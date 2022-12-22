from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSCreateSymlinkOperator(BaseOperator):
    """
    Create a symlnik file

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo
    :type repo: str
    :param branch: The lakeFS branch name
    :type branch: str
    :param location: Location where symlink will be created (optional)
    :type location: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        "repo",
        "branch",
    ]
    template_ext = ()
    ui_color = "#f4a460"

    @apply_defaults
    def __init__(
        self,
        lakefs_conn_id: str,
        repo: str,
        branch: str,
        location: str = None,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.branch = branch

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info(
            "Create symlink file for branch '%s' in repo '%s'", self.branch, self.repo
        )

        api_kwargs = {}
        if location:
            self.log.info("Create symlink in location '%s'", location)
            api_kwargs["location"] = location

        location = hook.create_symlink_file(self.repo, self.branch, **api_kwargs)

        return location
