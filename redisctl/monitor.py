import time
from datetime import datetime
import threading
import logging
from redistrib.clusternode import Talker, pack_command
from socket import error as SocketError

import instance_manage as im

CMD_INFO_MEM = pack_command('info', 'memory')
CMD_INFO_CPU = pack_command('info', 'cpu')


class Monitor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.cached_instances = dict()
        self.last_update = datetime.now()
        self.poll_count = 0

    @staticmethod
    def _reload_saved_instance():
        return {
            k: {
                'host': i[im.COL_HOST],
                'port': i[im.COL_PORT],
                'max_mem': i[im.COL_MEM],
                'stat': i[im.COL_STAT] >= 0,
                'free': i[im.COL_APPID] is None,
            } for k, i in im.InstanceManager.load_saved_instaces().iteritems()}

    @staticmethod
    def _info_mem(host, port):
        t = Talker(host, port)
        mem = dict()
        cpu = dict()
        ok = True
        try:
            for line in t.talk_raw(CMD_INFO_MEM).split('\n'):
                if len(line) == 0 or line.startswith('#'):
                    continue
                k, v = line.split(':')
                mem[k.strip()] = v.strip()
            for line in t.talk_raw(CMD_INFO_CPU).split('\n'):
                if len(line) == 0 or line.startswith('#'):
                    continue
                k, v = line.split(':')
                cpu[k.strip()] = v.strip()
        except StandardError, e:
            logging.error('Fail to retrieve info of %s:%d', host, port)
            logging.exception(e)
            ok = False
        finally:
            t.close()
        return mem, cpu, ok

    def run(self):
        while True:
            if self.poll_count % 8 == 0:
                logging.info('Poll %d - reload from database', self.poll_count)
                instances = Monitor._reload_saved_instance()
            else:
                instances = self.cached_instances

            for host_port, instance in instances.iteritems():
                try:
                    mem, cpu, ok = Monitor._info_mem(*host_port)
                    if ok:
                        instance['memory'] = mem
                        instance['cpu'] = cpu
                    else:
                        instance['stat'] = False
                except SocketError, e:
                    logging.error('Fail to connect to %s:%d', *host_port)
                    logging.exception(e)
                    instance['stat'] = False
            logging.info('Total %d instances', len(instances))
            self.cached_instances = instances

            self.poll_count += 1
            time.sleep(16)
