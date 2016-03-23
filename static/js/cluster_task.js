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
