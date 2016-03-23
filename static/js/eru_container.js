function delContainer() {
    if (!confirm('确定要下线此容器吗?')) {
        return;
    }
    var self = $(this).attr('disabled', 'disabled');
    $.ajax({
        url: '/containerize/remove',
        method: 'POST',
        data: {
            id: self.data('cid'),
            type: self.data('type')
        },
        success: function() {
            window.location.reload();
        },
        error: function(e) {
            self.text(_('failed') + ':' + e.responseText);
        }
    });
}

$(document).ready(function() {
    $('.btn-del-container').click(delContainer);
    $('.btn-revive-container').click(function () {
        var self = $(this).attr('disabled', 'disabled');
        $.ajax({
            url: '/containerize/revive',
            method: 'POST',
            data: {id: self.data('cid')},
            success: function() {
                window.location.reload();
            },
            error: function(e) {
                self.text(_('failed') + ':' + e.responseText);
            }
        });
    });
});
