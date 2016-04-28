function prune(self, url) {
    self.attr('disabled', 'disabled');
    $.ajax({
        type: 'POST',
        url: url,
        data: {id: self.data('id')},
        success: function() { self.text(_('done')); },
        error: function(e) {
            console.error(e);
            alert(_('failed'));
        }
    });
}
