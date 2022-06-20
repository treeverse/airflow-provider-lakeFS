from typing import Any, Dict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSGetObjectOperator(BaseOperator):
    """
    Get text of an object from lakeFS.  Reads into memory and only works for a *small* object!

    :param lakefs_conn_id: connection to run the operator with
    :type lakefs_conn_id: str
    :param repo: The lakeFS repo from which to get.
    :type repo: str
    :param ref: The reference from which to get.
    :type ref: str
    :param path: The path from which to get.
    :type path: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'repo',
        'ref',
        'path',
    ]

    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, lakefs_conn_id: str, repo: str, ref: str, path: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lakefs_conn_id = lakefs_conn_id
        self.repo = repo
        self.ref = ref
        self.path = path

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = LakeFSHook(lakefs_conn_id=self.lakefs_conn_id)

        self.log.info("Get object from repo '%s' reference '%s' path '%s'",
                      self.repo, self.ref, self.path)

        contents = hook.get_object(self.repo, self.ref, self.path)

        return str(contents.read(), 'utf-8')
