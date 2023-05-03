from airflow.models import BaseOperatorLink, XCom
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.utils.context import Context

import logging


log = logging.getLogger(__name__)


LAKEFS_COMMIT_LINK = "{base_url}/repositories/{repo}/commits/{commit_digest}"


class LakeFSLink(BaseOperatorLink):
    key = "lakefs_commit"
    format_str = LAKEFS_COMMIT_LINK
    name = "lakeFS"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        log.info(f"Search lakeFS link instance key {ti_key}")
        if not conf:
            log.error(f"Missing lakeFS link XCom for key {self.key} instance key {ti_key}")
            return ""
        return self.format_str.format(**conf)

    @staticmethod
    def persist(
            context: Context,
            task_instance: TaskInstance,
            lakefs_base_url: str,
            repo: str,
            commit_digest: str,
    ):
        value = {'base_url': lakefs_base_url, 'repo': repo, 'commit_digest': commit_digest}
        log.info(f"Persist lakeFS commit data {value}")
        task_instance.xcom_push(context, key=LakeFSLink.key, value=value)

LakeFSLink.operators = ["lakefs_provider.operators.commit_operator.LakeFSCommitOperator",
                        "lakefs_provider.operators.commit_operator.LakeFSMergeOperator"]
