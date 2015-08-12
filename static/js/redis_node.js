$(document).ready(function() {
    $('.fix-migrating-btn').click(function() {
        var btn = $(this);
        $.post('/nodes/fixmigrating', {
            host: btn.data('host'),
            port: btn.data('port')
        }, function() {
            window.location.reload();
        });
    });

    $('.node-deleter').click(function() {
        var btn = $(this);
        $.post('/nodes/del', {
            host: btn.data('host'),
            port: btn.data('port')
        }, function() {
            btn.parent().html('节点已被移除');
            $('button,.panel-div').remove();
        });
    });
});
