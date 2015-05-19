import redistrib.command

import file_ipc


def masters_info(host, port):
    node_details = {(n['host'], n['port']): n
                    for n in file_ipc.read()['nodes']}
    result = []
    masters, myself = redistrib.command.list_masters(host, port)
    for n in masters:
        r = {'host': n.host, 'port': n.port}
        if (n.host, n.port) in node_details:
            r['slots_count'] = len(node_details[(n.host, n.port)]['slots'])
        result.append(r)
    return result, myself
