function prune(self, url, data) {
    data = data || {};
    data.id = self.data('id');
    self.attr('disabled', 'disabled');
    $.ajax({
        type: 'POST',
        url: url,
        data: data,
        success: function() { self.text(_('done')); },
        error: function(e) {
            console.error(e);
            alert(_('failed'));
        }
    });
}
