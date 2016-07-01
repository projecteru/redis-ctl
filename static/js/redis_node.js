$(document).ready(function() {
    $('.fix-migrating-btn').click(taskFixMigrating);
    $('.node-deleter').click(function() {
        var btn = $(this);
        $.post('/redis/del', {
            host: btn.data('host'),
            port: btn.data('port')
        }, function() {
            btn.parent().html(_('Redis unregistered'));
            $('button,.panel-div').remove();
        });
    });
});

function cmdInfo(host, port, onText, onBlocks, onError) {
    $.get('/cmd/info', {host: host, port: port}, function(r) {
        onText(r);
        var lines = r.split('\n');
        var blocks = [{items: []}];
        for (var i = 0; i < lines.length; ++i) {
            var ln = lines[i].trim();
            if (!ln) {
                continue;
            }
            if (ln[0] === '#') {
                blocks.push({
                    title: ln.slice(1).trim(),
                    items: []
                });
                continue;
            }
            var k = ln.indexOf(':');
            if (-1 === k) {
                continue;
            }
            blocks[blocks.length - 1].items.push({
                key: ln.slice(0, k),
                value: ln.slice(k + 1)
            });
        }
        onBlocks(blocks);
    }).error(onError);
}

function cmdGetMaxMemory(host, port, onMaxMemory, onNotSet, onError) {
    $.get('/cmd/get_max_mem', {host: host, port: port}, function(r) {
        var m = parseInt(r[1]);
        if (m === 0) {
            return onNotSet();
        }
        onMaxMemory(m);
    }).error(onError);
}

function sortNodeByAddr(a, b) {
    if (a.host == b.host) {
        return a.port - b.port;
    }
    return a.host < b.host ? -1 : 1;
}

function parseClusterNodes(text) {
    function parseOne(parts) {
        if (parts.length < 8) {
            return;
        }
        var myself = false;
        var flags = parts[2].split(',');
        for (var j = 0; j < flags.length; ++j) {
            if (flags[j] === 'handshake') {
                return;
            }
            if (flags[j] === 'myself') {
                myself = true;
            }
        }
        var host_port = parts[1].split(':');
        var slots = [];
        var migrating = false;
        for (j = 8; j < parts.length; ++j) {
            if (parts[j][0] == '[') {
                migrating = true;
                continue;
            }
            var slots_range = parts[j].split('-');
            if (slots_range.length === 1) {
                slots.push(parseInt(slots_range[0]));
                continue;
            }
            for (var s = parseInt(slots_range[0]); s <= parseInt(slots_range[1]); ++s) {
                slots.push(s);
            }
        }
        return {
            node_id: parts[0],
            address: parts[1],
            host: host_port[0],
            port: parseInt(host_port[1]),
            flags: parts[2].split(','),
            myself: myself,
            slave: parts[3] !== '-',
            master_id: parts[3] === '-' ? null : parts[3],
            migrating: migrating,
            slots: slots,
            slots_text: parts.slice(8).join(' '),
            stat: true
        };
    }

    var lines = text.split('\n');
    var parts;
    var nodes = [];
    for (var i = 0; i < lines.length; ++i) {
        if (!lines[i]) {
            continue;
        }
        var n = parseOne(lines[i].split(' '));
        if (n) {
            nodes.push(n);
        }
    }
    return nodes;
}

function rolesLabels(roles) {
    return roles.map(function(e) {
        var color = null;
        var text = e;
        switch(e) {
        case 'myself':
            color = 'default';
            text = _('myself');
            break;
        case 'master':
            color = 'primary';
            text = _('master');
            break;
        case 'slave':
            color = 'warning';
            text = _('slave');
            break;
        case 'fail':
            color = 'danger';
            text = 'F';
            break;
        }
        return bscp.label(text, color);
    });
}

function redisRelations(redisList) {
    var masters = {};
    var allMasters = [];
    var slaves = [];

    $.each(redisList, function(i, n) {
        if (n.slave) {
            return slaves.push(n);
        }
        allMasters.push(n);
        n.slaves = [];
        if (n.node_id) {
            return masters[n.node_id] = n;
        }
    });

    $.each(slaves, function(i, n) {
        if (masters[n.master_id]) {
            return masters[n.master_id].slaves.push(n);
        }
        allMasters.push(n);
    });

    allMasters.sort(sortNodeByAddr);
    return allMasters;
}
