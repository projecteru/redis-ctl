import redistrib.command

import file_ipc


def masters_detail(host, port):
    node_details = file_ipc.read_details()['nodes']
    result = []
    masters, myself = redistrib.command.list_masters(host, port)
    for n in masters:
        r = {'host': n.host, 'port': n.port}
        try:
            r['slots_count'] = len(node_details[
                '%s:%d' % (n.host, n.port)]['slots'])
        except KeyError:
            pass
        result.append(r)
    return result, myself
