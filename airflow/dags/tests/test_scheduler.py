import unittest


from airflow.models import DagBag

# def dagbag():
#     return DagBag()


class TestScheduler(unittest.TestCase):
    def test_dag_loaded(self):
        dag_bag = DagBag(dag_folder='.')
        dag = dag_bag.get_dag(dag_id="job_data_pipeline")
        self.assertTrue(len(dag_bag.import_errors) == 0)
        self.assertTrue(dag is not None)
        self.assertTrue(len(dag.tasks) == 13)


if __name__ == '__main__':
    unittest.main()
