$(document).ready(function() {
    var argsFormatters = {
        launch: function(args) {
            var result = [];
            for (var i = 0; i < args.host_port_list.length; ++i) {
                var a = args.host_port_list[i];
                result.push(a.host + ':' + a.port);
            }
            return result.join(' | ');
        },
        fix_migrate: function(args) {
            return args.host + ':' + args.port;
        },
        migrate: function(args) {
            return [_('from'), args.src_host + ':' + args.src_port, _('migrate_out'),
                    args.slots.length, _('slots_to'), args.dst_host + ':' + args.dst_port].join(' ');
        },
        join: function(args) {
            return args.newin_host + ':' + args.newin_port;
        },
        replicate: function(args) {
            return [_('从节点为'), args.slave_host + ':' + args.slave_port, _('主节点为'),
                    args.master_host + ':' + args.master_port].join(' ');
        },
        quit: function(args) {
            return args.host + ':' + args.port;
        }
    };

    function renderStatus(status, error, completion) {
        if (status === 'pending') {
            return $('<span>').addClass('label label-info').text(_('awaiting'));
        }
        if (status === 'running') {
            return $('<span>').addClass('label label-primary').text(_('processing'));
        }
        if (error) {
            return [$('<span>').addClass('label label-danger').text(_('failed')),
                    $('<span>').text(' ' + completion)];
        }
        return [$('<span>').addClass('label label-success').text(_('completed')),
                $('<span>').text(' ' + completion)];
    }

    $('#taskDetail').on('show.bs.modal', function(event) {
        var taskId = $(event.relatedTarget).data('taskid');
        $('#taskDetailId').text(taskId);
        $('#taskDetailLoaderPlaceholder').show();
        $('#taskDetailContent').hide();
        $.ajax({
            url: '/task/steps',
            type: 'GET',
            data: {id: taskId},
            success: function(r) {
                $('#taskDetailStepCount').text(r.length);
                $('#taskDetailSteps').html('');
                $.each(r, function(i, e) {
                    $('#taskDetailSteps').append($('<tr>'
                        ).append($('<td>').text(e.id)
                        ).append($('<td>').text(_('task_step_' + e.command))
                        ).append($('<td>').append(argsFormatters[e.command](e.args))
                        ).append($('<td>').append(e.start_time)
                        ).append($('<td>').append(renderStatus(e.status, e.exec_error, e.completion))
                        ));
                        if (e.exec_error) {
                            $('#taskDetailSteps').append($('<tr>').append(
                                $('<td>').attr('colspan', 5).append($('<pre>').css('text-align', 'left').text(e.exec_error))));
                        }
                });
                $('#taskDetailLoaderPlaceholder').hide();
                $('#taskDetailContent').show();
            },
            error: function(r) {
                console.error(r);
            }
        });
    });
});

function createAndLaunchCluster(descr, nodeList, callback) {
    $.post('/cluster/add', {descr: descr}, function(r) {
        $.ajax({
            url: '/task/launch',
            type: 'POST',
            data: JSON.stringify({
                nodes: nodeList,
                cluster: r
            }),
            success: function() {
                callback(null, r);
            },
            error: function(e) {
                callback(e);
            }
        });
    });
}

function replicateTask(masterHost, masterPort, slaveHost, slavePort, callback) {
    $.ajax({
        url: '/task/replicate',
        type: 'POST',
        data: {
            master_host: masterHost,
            master_port: masterPort,
            slave_host: slaveHost,
            slave_port: slavePort
        },
        success: function() {
            callback(null);
        },
        error: function(e) {
            callback(e);
        }
    });
}

function joinTask(clusterId, nodes, callback) {
    $.ajax({
        url: '/task/join',
        type: 'POST',
        data: JSON.stringify({
            cluster_id: clusterId,
            nodes: nodes
        }),
        success: function() {
            callback();
        },
        error: function(r) {
            callback(r);
        }
    });
}

function enableMultipleRedisOp(selects, callback) {
    var MAX_MASTERS = 8;
    var MAX_SLAVES = 2;

    $.get('/redis/list_free', {}, function(r) {
        function appendSelect(appendTo, cls, defaultText) {
            var select = $('<select>').addClass('form-control').addClass(cls);
            appendTo.append($('<div>').addClass('col-xs-3 control-label').append(select))
            select.append($('<option>').text(defaultText));
            for (var i = 0; i < r.length; ++i) {
                select.append($('<option>').data('host', r[i].host).data('port', r[i].port).text(r[i].host + ':' + r[i].port));
            }
            return select;
        }

        var row;
        for (var j = 0; j < MAX_MASTERS; ++j) {
            row = $('<div>').addClass('form-group select-row').append($('<label>').addClass('control-label col-xs-2').text(_('master') + ' #' + j));
            selects.append(row);
            appendSelect(row, 'master-select', _('Select master'));
            for (var i = 0; i < MAX_SLAVES; ++i) {
                appendSelect(row, 'slave-select', _('Select slave'));
            }
        }
        callback();
    });

    window.checkMultipleRedisSelection = function(callback) {
        var activated = [];
        $('.select-row .master-select :selected').each(function(i, e) {
            var self = $(e);
            if (self.data('host')) {
                activated.push(self.parent().parent().parent());
            }
        });
        if (activated.length === 0) {
            alert(_('At least one master should be selected'));
            return null;
        }

        var masters = [];
        var slaveries = [];
        for (var i = 0; i < activated.length; ++i) {
            var msel = activated[i].find('.master-select :selected');
            var mhost = msel.data('host');
            var mport = parseInt(msel.data('port'));
            masters.push({host: mhost, port: mport});
            activated[i].find('.slave-select :selected').each(function(i, e) {
                var self = $(e);
                if (self.data('host')) {
                    slaveries.push({
                        mhost: mhost,
                        mport: mport,
                        slhost: self.data('host'),
                        slport: self.data('port')
                    });
                }
            });
        }
        callback(masters, slaveries);
    };
}
