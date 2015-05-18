$(document).ready(function() {
    $('.card').attachCardExpand();

    $('.check-suppress-alert').enableLabelCheck({
        onClick: function(self) {
            var root = self.parent();
            while (root.length && !root.hasClass('alert-status-root')) {
                root = root.parent();
            }
            root.find('.alert-status').toggleClass('alert-enabled');
            $.post('/set_alert_status/' + self.data('ntype'), {
                host: self.data('host'),
                port: self.data('port'),
                suppress: self.prop('checked') ? '1' : '0'
            });
        }
    });

    $('.delete-proxy-btn').click(function() {
        var btn = $(this);
        btn.attr('disabled', 'disabled').text('请稍候');
        $.ajax({
            url: '/cluster/delete_proxy',
            type: 'POST',
            data: {
                host: btn.data('host'),
                port: btn.data('port')
            },
            success: function() {
                btn.text('此代理已被移除');
            },
            error: function(e) {
                btn.text('失败: ' + e.responseText);
            }
        });
    })

    $('.toggle-next').click(function() {$(this).next().toggle();}).next().hide();
});
