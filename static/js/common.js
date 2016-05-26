$(document).ready(function() {
    $('.check-suppress-alert').enableLabelCheck({
        onClick: function(self) {
            var root = self.parent();
            while (root.length && !root.hasClass('alert-status-root')) {
                root = root.parent();
            }
            root.toggleClass('alert-enabled');
            $.post('/set_alarm/' + self.data('ntype'), {
                host: self.data('host'),
                port: self.data('port'),
                suppress: self.prop('checked') ? '1' : '0'
            });
        }
    });
    $('.delete-proxy-btn').click(function() {
        var btn = $(this);
        btn.attr('disabled', 'disabled').text(_('Please wait'));
        $.ajax({
            url: '/cluster/delete_proxy',
            type: 'POST',
            data: {
                host: btn.data('host'),
                port: btn.data('port')
            },
            success: function() {
                btn.text(_('Proxy unregistered'));
            },
            error: function(e) {
                btn.text(_('failed') + ': ' + e.responseText);
            }
        });
    })

    $('.toggle-next').click(function() {$(this).next().toggle();}).next().hide();

    $('.panel-heading-hide-content').click(function() {
       $(this).next().slideToggle();
    });
});

window.TRANSLATIONS = window.TRANSLATIONS || {};
window._ = function(text) {
    return window.TRANSLATIONS[text] || text;
};
