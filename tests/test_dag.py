import os
from datetime import datetime

os.environ["MOCK_EDGAR"] = "true"

from dags.edgar_pipeline import dag


class TestDAGStructure:
    def test_dag_exists(self):
        assert dag is not None
        assert dag.dag_id == "edgar_pipeline"

    def test_dag_has_tasks(self):
        task_ids = [task.task_id for task in dag.tasks]
        assert len(task_ids) == 7
        expected_tasks = [
            "fetch_new_filings",
            "download_raw_documents",
            "parse_xbrl_facts",
            "validate_quality_gates",
            "load_to_warehouse",
            "update_audit_trail",
            "send_alerts_on_failure",
        ]
        for task_id in expected_tasks:
            assert task_id in task_ids

    def test_dag_task_dependencies(self):
        task_dict = {task.task_id: task for task in dag.tasks}

        fetch_task = task_dict["fetch_new_filings"]
        download_task = task_dict["download_raw_documents"]
        parse_task = task_dict["parse_xbrl_facts"]
        validate_task = task_dict["validate_quality_gates"]
        load_task = task_dict["load_to_warehouse"]
        audit_task = task_dict["update_audit_trail"]

        assert download_task in fetch_task.downstream_list
        assert parse_task in download_task.downstream_list
        assert validate_task in parse_task.downstream_list
        assert load_task in validate_task.downstream_list
        assert audit_task in load_task.downstream_list

    def test_dag_schedule(self):
        assert dag.schedule_interval == "@daily"

    def test_dag_catchup(self):
        assert dag.catchup is False

    def test_dag_start_date(self):
        assert dag.start_date == datetime(2024, 1, 1)

    def test_alert_trigger_rule(self):
        alert_task = None
        for task in dag.tasks:
            if task.task_id == "send_alerts_on_failure":
                alert_task = task
                break
        assert alert_task is not None
        assert alert_task.trigger_rule == "one_failed"
