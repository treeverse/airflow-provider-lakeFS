from typing import Any, Dict
from airflow.configuration import conf as airflow_conf
from airflow.models import BaseOperator
from sqlalchemy.exc import SQLAlchemyError


class WithLakeFSMetadataOperator(BaseOperator):

    __metadata_prefix = "::lakefs::Airflow::"

    # DAG metadata to add to commit, expanded after fetching appropriate
    # metadata.  The key of each metadata item will be prefixed
    # "::lakefs::".
    __metadata_templates = {
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

    __required_keys = ['dag', 'dag_run', 'logical_date', 'data_interval_start', 'data_interval_end', 'params']

    def _get_current_dag_dict(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Return a dict that describes parameters for the DAG run in context.

        Dict has keys 'endpoint_url', 'id', 'run_id', 'logical_date'.
        """
        ret = {}
        ret['endpoint_url'] = airflow_conf.get("webserver", "base_url")

        # TODO(ariels): Configurably filter metadata keys.
        for key in self.__required_keys:
            value = context.get(key, None)
            if value is None:
                self.log.warning(f"context does not contain {key}")
            ret[key] = value

        return ret

    def enrich_metadata(self, context: Dict[str, Any]):
        """Enrich metadata with values for lakeFS."""
        cdd = self._get_current_dag_dict(context)
        for k, template in self.__metadata_templates.items():
            try:
                expanded = str(self.render_template(template, cdd))
                self.metadata[self._metadata_key(k)] = expanded
            except SQLAlchemyError as sql_e:
                self.log.warning(f"metadata {k} not added: ${sql_e} (possibly undefined fields)")

    @classmethod
    def _metadata_key(cls, key: str) -> str:
        return cls.__metadata_prefix + key
