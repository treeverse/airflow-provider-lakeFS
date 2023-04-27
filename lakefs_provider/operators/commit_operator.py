from typing import Any, Dict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.helpers import build_airflow_url_with_query
from airflow.configuration import conf as airflow_conf

from sqlalchemy.exc import SQLAlchemyError

from lakefs_provider.hooks.lakefs_hook import LakeFSHook


class LakeFSCommitOperator(BaseOperator):
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

    metadata_prefix = "::lakefs::Airflow::"

    # DAG metadata to add to commit, expanded after fetching appropriate
    # metadata.  The key of each metadata item will be prefixed
    # "::lakefs::".
    metadata_templates = {
        "dag_run_id": "{{dag_run.run_id}}",
        "dag_id": "{{dag.dag_id}}",
        "logical_date": "{{logical_date}}",
        "data_interval_start[iso8601]": "{{data_interval_start}}",
        "data_interval_end[iso8601]": "{{data_interval_end}}",
        "last_scheduling_decision": "{{dag_run.last_scheduling_decision}}",
        "run_type": "{{dag_run.run_type}}",
        "external_trigger": "{{dag_run.external_trigger}}",
        "conf": "{{params}}",
        "note": "{{dag_run.note}}",
        # build_airflow_url_with_query can only run from a Flask app
        # context.  Fake URLs instead.
        "url[url:id]": '{{endpoint_url}}/api/v1/dags/{{dag.dag_id}}/dagRuns/{{dag_run.run_id}}',
        "url[url:ui]": '{{endpoint_url}}/dags/{{dag.dag_id}}/graph?dag_run_id={{dag_run.run_id}}&root=&logical_date={{logical_date}}',
    }
    required_keys = ['dag', 'dag_run', 'logical_date', 'data_interval_start', 'data_interval_end', 'params']

    def _get_current_dag_dict(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Return a dict that describes parameters for the DAG run in context.

        Dict has keys 'endpoint_url', 'id', 'run_id', 'logical_date'.
        """
        ret = {}
        ret['endpoint_url'] = airflow_conf.get("webserver", "base_url")

        for key in self.required_keys:
            value = context.get(key, None)
            if value is None:
                self.log.warning(f"context does not contain {key}")
            ret[key] = value

        return ret

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

        self.metadata.__setitem__("airflow_task_id", self.task_id)

        # TODO(ariels): Configurably filter metadata keys.

        cdd = self._get_current_dag_dict(context)
        self.log.info("[DEBUG] cdd", cdd)

        for k, template in self.metadata_templates.items():
            try:
                expanded = str(self.render_template(template, cdd))
                self.metadata.__setitem__(self.metadata_key(k), expanded)
            except SQLAlchemyError as sql_e:
                self.log.warning(f"metadata {k} not added: ${sql_e} (possibly undefined fields)")


        ref = hook.commit(self.repo, self.branch, self.msg, self.metadata)

        # TODO(ariels): Add lakeFS commit URL and commit UI URLs as links!

        return ref

    @classmethod
    def metadata_key(cls, key: str) -> str:
        return cls.metadata_prefix + key
