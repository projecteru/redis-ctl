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

    function findDt(t) {
        var self = $(t);
        if (self.hasClass('dd')) {
            self = self.prev();
        }
        if (!self.hasClass('dt')) {
            return null;
        }
        return self;
    }

    function dtSetScheme(self, sc) {
        return self.removeClass('dt-success dt-info dt-primary dt-warning dt-danger dt-default').addClass('dt-' + sc);
    }

    $.fn.dtResetDefault = function() {
        return this.each(function() {
            var self = findDt(this);
            if (self === null) {
                return;
            }
            dtSetScheme(self, 'default');
        });
    };

    $.fn.dtSetScheme = function(sc) {
        return this.each(function() {
            var self = findDt(this);
            if (self === null) {
                return;
            }
            dtSetScheme(self, sc);
        });
    };
}(jQuery);

var bscp = {
    form: function(id) {
        return $('<div>').attr('id', id).addClass('form-horizontal');
    },
    row: function(id) {
        return $('<div>').attr('id', id).addClass('form-group');
    },
    grid: function(size, offset, id) {
        var cls = ''
        if (offset > 0) {
            cls += ' col-xs-offset-' + offset;
        }
        return $('<div>').attr('id', id).addClass('col-xs-' + size + cls);
    },
    clabel: function(size, offset, id) {
        var cls = ''
        if (offset > 0) {
            cls += ' col-xs-offset-' + offset;
        }
        return $('<div>').attr('id', id).addClass('control-label col-xs-' + size + cls);
    },
    label: function(text, color, id) {
        var cls = ''
        if (color) {
            cls += ' label-' + color;
        } else {
            cls += ' label-default';
        }
        return $('<div>').attr('id', id).addClass('label' + cls).text(text);
    },
    btn: function(text, color, cls, id) {
        cls = (cls || '') + ' btn';
        if (color) {
            cls += ' btn-' + color;
        }
        return $('<button>').attr('type', 'button').attr('id', id).addClass(cls).text(text);
    }
};
