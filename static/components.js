-function($) {
    var CHECKED_CLASS = 'fa-check-circle-o';
    var UNCHECKED_CLASS = 'fa-circle-o';

    $.fn.enableLabelCheck = function(opt) {
        opt = opt || {};
        var onClick = opt.onClick || function() {};

        return this.each(function() {
            var self = $(this);

            var fa = $('<i>').addClass('fa');
            self.prepend(fa);

            if (self.hasClass('check-group-checked')) {
                self.prepend(fa.addClass(CHECKED_CLASS));
            } else {
                self.prepend(fa.addClass(UNCHECKED_CLASS));
            }
            self.click(function() {
                self.toggleClass('check-group-checked');
                fa.toggleClass(CHECKED_CLASS);
                fa.toggleClass(UNCHECKED_CLASS);
                onClick(self, self.hasClass('check-group-checked'));
            });
        });
    }
}(jQuery);
