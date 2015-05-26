# coding: utf-8

import json
import socket
import requests
import urllib
import urlparse
from urlparse import urljoin

session = requests.Session()

class EruClient(object):

    def __init__(self, url, timeout=5, username='', password=''):
        self._url = url
        self._timeout = timeout
        self._username = username
        self._password = password

    def request(self, url, method='GET', params=None, data=None, as_json=True):
        headers = {'content-type': 'application/json'}
        if params is None:
            params = {}
        if data is None:
            data = {}
        if 'start' not in params:
            params['start'] = 0
        if 'limit' not in params:
            params['limit'] = 20
        target_url = urljoin(self._url, url)
        try:
            resp = session.request(method=method, url=target_url, params=params,
                    data=json.dumps(data), timeout=self._timeout, headers=headers)
            if as_json:
                return resp.json()
            return resp.content
        except requests.exceptions.ReadTimeout:
            if as_json:
                return {'r': 1, 'msg': 'Read timeout'}
            return 'Read timeout'
        except requests.exceptions.ConnectionError:
            if as_json:
                return {'r': 1, 'msg': 'Connection refused'}
            return 'Connection refused'

    def post(self, url, params=None, data=None, as_json=True):
        return self.request(url, 'POST', params=params, data=data, as_json=as_json)

    def put(self, url, params=None, data=None, as_json=True):
        return self.request(url, 'PUT', params=params, data=data, as_json=as_json)

    def get(self, url, params=None, data=None, as_json=True):
        return self.request(url, 'GET', params=params, data=data, as_json=as_json)

    def delete(self, url, params=None, data=None, as_json=True):
        return self.request(url, 'DELETE', params=params, data=data, as_json=as_json)

    def register_app_version(self, name, version, git, token, appyaml):
        url = '/api/app/register/'
        data = {
            'name': name,
            'version': version,
            'git': git,
            'token': token,
            'appyaml': appyaml,
        }
        return self.post(url, data=data)

    def set_app_env(self, name, env, **kv):
        url = '/api/app/{0}/env/'.format(name)
        data = {'env': env}
        data.update(kv)
        return self.put(url, data=data)

    def list_app_env_content(self, name, env):
        url = '/api/app/{0}/env/'.format(name)
        params = {'env': env}
        return self.get(url, params=params)

    def list_app_env_names(self, name):
        url = '/api/app/{0}/listenv'.format(name)
        return self.get(url)

    def get_app(self, name):
        url = '/api/app/{0}'.format(name)
        return self.get(url)

    def get_version(self, name, version):
        url = '/api/app/{0}/{1}'.format(name, version)
        return self.get(url)

    def list_app_containers(self, name):
        url = '/api/app/{0}/containers/'.format(name)
        return self.get(url)

    def deploy_private(self, group_name, pod_name, app_name, ncore,
            ncontainer, version, entrypoint, env, network_ids):
        url = '/api/deploy/private/{0}/{1}/{2}'.format(group_name, pod_name, app_name)
        data = {
            'ncore': ncore,
            'ncontainer': ncontainer,
            'version': version,
            'entrypoint': entrypoint,
            'env': env,
            'networks': network_ids,
        }
        return self.post(url, data=data)

    def deploy_public(self, group_name, pod_name, app_name, ncontainer,
            version, entrypoint, env, network_ids):
        url = '/api/deploy/public/{0}/{1}/{2}'.format(group_name, pod_name, app_name)
        data = {
            'ncontainer': ncontainer,
            'version': version,
            'entrypoint': entrypoint,
            'env': env,
            'networks': network_ids,
        }
        return self.post(url, data=data)

    def build_image(self, group_name, pod_name, app_name, base, version):
        url = '/api/deploy/build/{0}/{1}/{2}'.format(group_name, pod_name, app_name)
        data = {
            'base': base,
            'version': version,
        }
        return self.post(url, data=data)

    def rm_containers(self, container_ids):
        return self.put('/api/deploy/rmcontainers/', data={
            'cids': container_ids,
        })

    def offline_version(self, group_name, pod_name, app_name, version):
        url = '/api/deploy/rmversion/{0}/{1}/{2}'.format(group_name, pod_name, app_name)
        data = {'version': version}
        return self.post(url, data=data)

    def remove_containers(self, group_name, pod_name, app_name, version, host, ncontainer=1):
        url = '/api/deploy/rmcontainer/{0}/{1}/{2}'.format(group_name, pod_name, app_name)
        data = {
            'version': version,
            'host': host,
            'ncontainer': ncontainer,
        }
        return self.post(url, data=data)

    def update_version(self, group_name, pod_name, app_name, version):
        url = '/api/deploy/updateversion/{0}/{1}/{2}'.format(group_name, pod_name, app_name)
        data = {'version': version}
        return self.put(url, data=data)

    def version(self):
        url = '/'
        return self.get(url, as_json=False)

    def kill_container(self, container_id):
        url = '/api/container/{0}/kill'.format(container_id)
        return self.put(url)

    def poll_container(self, container_id):
        url = '/api/container/{0}/poll'.format(container_id)
        return self.get(url)

    def create_group(self, name, description):
        url = '/api/sys/group/create'
        data = {
            'name': name,
            'description': description,
        }
        return self.post(url, data=data)

    def create_pod(self, name, description):
        url = '/api/sys/pod/create'
        data = {
            'name': name,
            'description': description,
        }
        return self.post(url, data=data)

    def assign_pod_to_group(self, pod_name, group_name):
        url = '/api/sys/pod/{0}/assign'.format(pod_name)
        data = {'group_name': group_name}
        return self.post(url, data=data)

    def create_host(self, addr, pod_name):
        url = '/api/sys/host/create'
        data = {
            'addr': addr,
            'pod_name': pod_name,
        }
        return self.post(url, data=data)

    def assign_host_to_group(self, addr, group_name):
        url = '/api/sys/host/{0}/assign'.format(addr)
        data = {'group_name': group_name}
        return self.post(url, data=data)

    def get_group_max_priviate_containers(self, group_name, pod_name, ncore):
        url = '/api/sys/group/{0}/available_container_count'.format(group_name)
        params = {'pod_name': pod_name, 'ncore': ncore}
        return self.get(url, params=params)

    def alloc_resource(self, appname, env, res_name, res_alias):
        url = '/api/app/alloc/{0}/{1}/{2}/{3}/'.format(
            appname, env, res_name, res_alias,
        )
        data = {
            'dbname': appname,
            'username': appname,
            'pass_len': 8,
        }
        return self.post(url, data=data)

    def create_network(self, name, netspace):
        url = '/api/network/create/'
        data = {
            'name': name,
            'netspace': netspace,
        }
        return self.post(url, data=data)

    def list_network(self, start=0, limit=20):
        url = '/api/network/list/'
        return self.get(url)

    def get_network_by_name(self, network_name):
        url = '/api/network/{0}/'.format(network_name)
        return self.get(url)

    def get_task(self, task_id):
        return self.get('/api/task/%d/' % task_id)

    def container_info(self, container_id):
        return self.get('/api/container/%s/' % container_id)

    def get_versions(self, app):
        return self.get('/api/app/%s/versions/' % app)

    def list_pods(self):
        return self.get('/api/pod/list/')
