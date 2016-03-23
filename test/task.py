import json
import redistrib.command as comm

import base
from models.task import ClusterTask, TASK_TYPE_LAUNCH


class Task(base.TestCase):
    def test_execution(self):
        with self.app.test_client() as client:
            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertReqStatus(200, r)
            r = client.post('/cluster/add', data={
                'descr': 'lazy dog',
            })
            self.assertReqStatus(200, r)
            cluster_id = r.data

            r = client.post('/task/launch', data=json.dumps({
                'cluster': cluster_id,
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                }],
            }))
            self.assertReqStatus(200, r)
            self.exec_all_tasks()

            task = ClusterTask(cluster_id=int(cluster_id), task_type=0)
            task.add_step(
                'join', cluster_id=cluster_id, cluster_host='127.0.0.1',
                cluster_port=7100, newin_host='127.0.0.1', newin_port=7101)
            task.add_step(
                'migrate', src_host='127.0.0.1', src_port=7100,
                dst_host='127.0.0.1', dst_port=7101, slots=[0, 1])
            self.db.session.add(task)
            self.db.session.commit()

            self.exec_all_tasks()

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))
            self.assertEqual(range(2, 16384), sorted(node_7100.assigned_slots))

            tasks = list(self.db.session.query(ClusterTask).order_by(
                ClusterTask.id.asc()).all())
            self.assertEqual(2, len(tasks))
            t = tasks[0]
            self.assertIsNotNone(t.completion)
            self.assertEqual(TASK_TYPE_LAUNCH, t.task_type)

            t = tasks[1]
            self.assertIsNotNone(t.completion)
            self.assertIsNone(t.exec_error)
            self.assertIsNone(t.acquired_lock())

            comm.quit_cluster('127.0.0.1', 7101)
            comm.shutdown_cluster('127.0.0.1', 7100)

    def test_execution_failed(self):
        with self.app.test_client() as client:
            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertReqStatus(200, r)
            r = client.post('/cluster/add', data={
                'descr': 'lazy dog',
            })
            self.assertReqStatus(200, r)
            cluster_id = r.data

            r = client.post('/task/launch', data=json.dumps({
                'cluster': cluster_id,
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                }],
            }))
            self.assertReqStatus(200, r)
            self.exec_all_tasks()

            task = ClusterTask(cluster_id=int(cluster_id), task_type=0)
            task.add_step(
                'join', cluster_id=cluster_id, cluster_host='127.0.0.1',
                cluster_port=7100, newin_host='127.0.0.1', newin_port=7101)
            task.add_step(
                'migrate', src_host='127.0.0.1', src_port=7100,
                dst_host='127.0.0.1', dst_port=7101, slots=[0, 1])
            task.add_step(
                'migrate', src_host='127.0.0.1', src_port=7100,
                dst_host='127.0.0.1', dst_port=7101, slots=[0, 1])
            task.add_step(
                'migrate', src_host='127.0.0.1', src_port=7100,
                dst_host='127.0.0.1', dst_port=7101, slots=[2, 3])
            self.db.session.add(task)
            self.db.session.commit()

            self.exec_all_tasks()

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))
            self.assertEqual(range(2, 16384), sorted(node_7100.assigned_slots))

            tasks = list(self.db.session.query(ClusterTask).order_by(
                ClusterTask.id.asc()).all())
            self.assertEqual(2, len(tasks))
            t = tasks[1]
            self.assertIsNotNone(t.completion)
            self.assertIsNotNone(t.exec_error)
            self.assertIsNone(t.acquired_lock())

            steps = t.all_steps
            self.assertEqual(4, len(steps))
            step = steps[0]
            self.assertTrue(step.completed)
            self.assertIsNone(step.exec_error)
            step = steps[1]
            self.assertTrue(step.completed)
            self.assertIsNone(step.exec_error)
            step = steps[2]
            self.assertTrue(step.completed)
            self.assertIsNotNone(step.exec_error)
            step = steps[3]
            self.assertFalse(step.started)
            self.assertFalse(step.completed)
            self.assertIsNone(step.exec_error)

            comm.quit_cluster('127.0.0.1', 7101)
            comm.shutdown_cluster('127.0.0.1', 7100)
