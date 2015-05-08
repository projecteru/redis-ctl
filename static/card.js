-function() {
    $.fn.attachCardExpand = function() {
        return this.each(function() {
            var self = $(this);
            var deactive = self.find('.card-deactive');
            if (deactive.length == 0) {
                deactive = $('<button>').addClass('btn').addClass('card-deactive').insertAfter(self.find('.card-title'));
            }
            deactive.addClass('card-detailonly');
            deactive.append($('<i>').addClass('fa').addClass('fa-minus-square'));
            deactive.click(function(e) {
                e.stopPropagation();
                toPreviewMode(self);
            });
            self.click(function() {
                if (self.hasClass('card-active')) {
                    return;
                }
                toDetailMode(self);
            });
            return self;
        });
    };

    function getCommonComponents(self) {
        var commonComponents = {};
        self.find('[card-detailto]').each(function() {
            commonComponents[$(this).attr('card-detailto')] = $(this);
        });
        return commonComponents;
    }

    function toDetailMode(self) {
        self.removeClass('card').addClass('card-active');
        var commonComponents = getCommonComponents(self);
        self.find('[card-detailfrom]').each(function() {
            var detailfrom = $(this).attr('card-detailfrom');
            var c = commonComponents[detailfrom];
            if (!c) {
                return;
            }
            var placeholder = $('<span>').attr('card-detailrecover', detailfrom);
            placeholder.insertAfter(c);
            c.detach();
            $(this).append(c);
        });
    }

    function toPreviewMode(self) {
        var commonComponents = getCommonComponents(self);
        self.find('[card-detailrecover]').each(function() {
            var recover = $(this).attr('card-detailrecover');
            var c = commonComponents[recover];
            if (!c) {
                return;
            }
            $(this).replaceWith(c);
        });
        self.addClass('card').removeClass('card-active');
    }
}();
