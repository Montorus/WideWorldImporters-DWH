import unittest
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_FOLDER = PROJECT_ROOT / "airflow" / "dags"
if Path("/opt/airflow/dags").exists():
    DAGS_FOLDER = Path("/opt/airflow/dags")


class AirflowDagImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)

    def test_dag_files_import_without_errors(self):
        self.assertEqual({}, self.dagbag.import_errors)

    def test_expected_dags_are_registered(self):
        expected_dag_ids = {
            "etl_mssql_customers",
            "etl_postgresql_adventureworks",
        }

        self.assertTrue(expected_dag_ids.issubset(self.dagbag.dags.keys()))

    def test_expected_tasks_are_registered(self):
        expected_tasks = {
            "etl_mssql_customers": {"extract_and_load_customers"},
            "etl_postgresql_adventureworks": {"extract_and_load_adventureworks"},
        }

        for dag_id, task_ids in expected_tasks.items():
            dag = self.dagbag.get_dag(dag_id)
            self.assertIsNotNone(dag)
            self.assertEqual(task_ids, {task.task_id for task in dag.tasks})


if __name__ == "__main__":
    unittest.main()
