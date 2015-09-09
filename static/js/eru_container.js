function delContainer() {
    var self = $(this).attr('disabled', 'disabled');
    $.ajax({
        url: '/nodes/delete/eru',
        method: 'POST',
        data: {
            id: self.data('cid'),
            type: self.data('type')
        },
        success: function() {
            window.location.reload();
        },
        error: function(e) {
            self.text('发生错误: ' + e.responseText);
        }
    });
}

$(document).ready(function() {
    $('.btn-del-container').click(delContainer);
});
