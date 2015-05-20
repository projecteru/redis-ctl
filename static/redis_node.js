function bindMemoryTrans(parentNode) {
    var input = parentNode.find('input').data('mem', NaN);
    var parsedMem = parentNode.find('.mem-parsed');
    var errorSpan = parentNode.find('.mem-error');

    input.blur(function() {
        errorSpan.text('');
        parsedMem.text('-');
        input.data('mem', NaN);

        var mem = input.val();
        var base = parseFloat(mem);
        if (isNaN(base) || base <= 0) {
            return errorSpan.text('不正确的内存大小格式');
        }
        var factor = 1;
        switch (mem[mem.length - 1]) {
            case 'k':
            case 'K':
                factor = 1024;
                break;
            case 'm':
            case 'M':
                factor = 1024 * 1024;
                break;
            case 'g':
            case 'G':
                factor = 1024 * 1024 * 1024;
                break;
            default:
        }
        base *= factor;
        parsedMem.text(base);
        if (base % 1 != 0) {
            return errorSpan.text('内存大小不是一个整数');
        }
        input.data('mem', base);
    });
}
