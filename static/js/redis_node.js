$(document).ready(function() {
    $('.fix-migrating-btn').click(function() {
        var btn = $(this);
        $.post('/task/fix_redis', {
            host: btn.data('host'),
            port: btn.data('port')
        }, function() {
            window.location.reload();
        });
    });

    $('.node-deleter').click(function() {
        var btn = $(this);
        $.post('/redis/del', {
            host: btn.data('host'),
            port: btn.data('port')
        }, function() {
            btn.parent().html(_('节点已被移除'));
            $('button,.panel-div').remove();
        });
    });
});
