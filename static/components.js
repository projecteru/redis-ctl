-function($) {
    $.fn.enableLabelCheck = function(opt) {
        opt = opt || {};
        var callback = opt.onClick || function() {};
        var checkedClass = opt.checkedClass || 'fa-check-square-o';
        var uncheckedClass = opt.uncheckedClass || 'fa-square-o';
        return this.each(function() {
            var self = $(this);
            var fa = $('<i>').addClass('fa');
            self.prepend(fa);

            if (self.hasClass('check-group-checked')) {
                self.prepend(fa.addClass(checkedClass));
            } else {
                self.prepend(fa.addClass(uncheckedClass));
            }
            self.click(function() {
                self.toggleClass('check-group-checked');
                fa.toggleClass(uncheckedClass);
                fa.toggleClass(checkedClass);
                self.prop('checked', self.hasClass('check-group-checked'));
                callback(self);
            });
        });
    };
}(jQuery);
