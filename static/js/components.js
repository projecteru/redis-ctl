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
            self.prop('checked', self.hasClass('check-group-checked'));
            if (self.prop('checked')) {
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

    $.fn.enableLabelSelect = function(opt) {
        opt = opt || {};
        var callback = opt.onChange || function() {};
        var mainWidth = opt.width || 120;
        var listWidth = opt.listWidth || mainWidth;
        var itemWidth = opt.itemWidth || listWidth;

        function fillList(select, selectDiv, ul) {
            var children = select.children('option');
            var nopts = children.length;
            var text = select.data('default-text') || children.eq(0).text();
            for (var i = 0; i < nopts; ++i) {
                var option = children.eq(i);
                if (option.val()) {
                    $('<li>').text(option.text()).data('rel', option.val()).click(function(e) {
                        e.stopPropagation();
                        selectDiv.text($(this).text()).removeClass('active');
                        select.val($(this).data('rel'));
                        ul.hide();
                        callback(select.val(), select);
                    }).css('width', itemWidth).appendTo(ul);
                } else {
                    text = option.text();
                }
                if (option.prop('selected')) {
                    text = option.text();
                }
            }
            selectDiv.text(text);
        }

        return this.each(function() {
            var self = $(this);
            if (self.parent().hasClass('select-wrap') && self.parent().find('ul').length === 1) {
                return fillList(self, self.parent().find('div.select-styled'), self.parent().find('ul').html(''));
            }
            self.addClass('select-hidden');
            self.wrap($('<div>').css('width', mainWidth).addClass('select-wrap').addClass(self.data('select-style')));
            var selectDiv = $('<div>').addClass('select-styled').addClass(self.data('select-style'));
            self.after(selectDiv);

            selectDiv.click(function(e) {
                e.stopPropagation();
                if ($(this).hasClass('active')) {
                    return cancel();
                }
                $('div.select-styled.active').each(function(){
                    $(this).removeClass('active').next('ul.select-options').hide();
                });
                $(this).toggleClass('active').next('ul.select-options').toggle();
            });

            var ul = $('<ul>').addClass('select-options').insertAfter(selectDiv).css('width', listWidth);
            fillList(self, selectDiv, ul);
            function cancel() {
                selectDiv.removeClass('active');
                ul.hide();
            }
            $(document).click(cancel);
        });
    };
}(jQuery);
