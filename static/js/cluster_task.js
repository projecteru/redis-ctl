$(document).ready(function() {
    var STEP_TYPES = {
        fix_migrate: '修复迁移状态',
        migrate: '迁移槽位',
        join: '添加主节点',
        replicate: '添加从节点',
        quit: '移除节点'
    };

    var argsFormatters = {
        fix_migrate: function(args) {
            return args.host + ':' + args.port;
        },
        migrate: function(args) {
            return ['从 ', args.src_host, ':', args.src_port, ' 迁移 ',
                    args.slots.length, ' 个槽位至 ', args.dst_host, ':',
                    args.dst_port].join('');
        },
        join: function(args) {
            return args.newin_host + ':' + args.newin_port;
        },
        replicate: function(args) {
            return ['从节点为 ', args.slave_host, ':', args.slave_port, ' 主节点为 ',
                    args.master_host, ':', args.master_port].join('');
        },
        quit: function(args) {
            return args.host + ':' + args.port;
        }
    };

    function renderStatus(status, error, completion) {
        if (status === 'pending') {
            return $('<span>').addClass('label label-info').text('等待');
        }
        if (status === 'running') {
            return $('<span>').addClass('label label-primary').text('正在执行');
        }
        if (error) {
            return [$('<span>').addClass('label label-danger').text('失败'),
                    $('<span>').text(' ' + completion)];
        }
        return [$('<span>').addClass('label label-success').text('完成'),
                $('<span>').text(' ' + completion)];
    }

    $('#taskDetail').on('show.bs.modal', function(event) {
        var taskId = $(event.relatedTarget).data('taskid');
        $('#taskDetailId').text(taskId);
        $('#taskDetailLoaderPlaceholder').show();
        $('#taskDetailContent').hide();
        $.ajax({
            url: '/cluster/task/steps',
            type: 'GET',
            data: {id: taskId},
            success: function(r) {
                $('#taskDetailStepCount').text(r.length);
                $('#taskDetailSteps').html('');
                $.each(r, function(i, e) {
                    $('#taskDetailSteps').append($('<tr>'
                        ).append($('<td>').text(e.id)
                        ).append($('<td>').text(STEP_TYPES[e.command])
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
