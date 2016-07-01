import json

import base
import models.task
from models.base import commit_session


class ClusterTest(base.TestCase):
    def test_create_delete_cluster(self):
        with self.app.test_client() as client:
            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertReqStatus(200, r)
            self.assertEqual({
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                    'suppress_alert': 1,
                }],
                'proxies': [],
            }, self.app.polling_targets())

        with self.app.test_client() as client:
            r = client.post('/cluster/add', data={
                'descr': 'the-quick-brown-fox',
            })
            self.assertReqStatus(200, r)
            cluster_id = int(r.data)

            r = client.post('/task/launch', data=json.dumps({
                'cluster': cluster_id,
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                }],
            }))
            self.assertReqStatus(200, r)
            self.exec_all_tasks()

        with self.app.test_client() as client:
            r = client.post('/cluster/shutdown', data={
                'cluster_id': cluster_id,
            })
            self.assertReqStatus(200, r)
            self.exec_all_tasks()

            tasks = models.task.ClusterTask.query.all()
            self.assertEqual(1, len(tasks))
            self.assertEqual(cluster_id, tasks[0].cluster_id)

        with self.app.test_client() as client:
            r = client.post('/cluster/delete', data={
                'id': cluster_id,
            })
            self.assertReqStatus(400, r)

            models.task.TaskStep.query.filter_by(task_id=tasks[0].id).delete()
            models.task.ClusterTask.query.delete()
            commit_session()

        with self.app.test_client() as client:
            r = client.post('/cluster/delete', data={
                'id': cluster_id,
            })
            self.assertReqStatus(200, r)
