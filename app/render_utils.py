from flask import render_template, Markup


def component(tp, **kwargs):
    return Markup(render_template('components/%s.html' % tp, **kwargs))


def g_icon(icon, color=None):
    return component('icon', icon=icon, color=color)


def g_label(text, size=2, offset=0, id=None, cls=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'lbl-' + id
    return component('label', text=text, size=size, offset=offset, id=id,
                     cls=cls or [], data=data or {}, lcl=lcl)


def g_hint(text, size=2, offset=0, id=None, cls=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'lbl-' + id
    return component('hint', text=text, size=size, offset=offset, id=id,
                     cls=cls or [], data=data or {}, lcl=lcl)


def g_input(size=2, offset=0, id=None, cls=None, value=None, placeholder=None,
            addon=None, readonly=False, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'input-' + id
    return component('input', size=size, offset=offset, id=id, cls=cls or [],
                     value=value or '', placeholder=placeholder or '',
                     addon=addon, readonly=readonly, data=data or {})


def g_select(size=1, offset=0, id=None, cls=None, value=None,
            addon=None, readonly=False, options=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'select-' + id
    return component('select', size=size, offset=offset, id=id, cls=cls or [],
                     value=value or '', addon=addon, readonly=readonly, options=options or [])


def g_button(text, size=2, offset=0, color='default', id=None, cls=None,
             icon=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'button-' + id
    return component('button', text=text, size=size, offset=offset, id=id,
                     color=color, cls=cls or [], icon=icon, data=data or {},
                     lcl=lcl)


def g_checkbox(text, size=2, offset=0, color='default', checked=False,
               id=None, cls=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'checkbox-' + id
    return component('checkbox', text=text, size=size, offset=offset,
                     color=color, checked=checked, id=id, cls=cls or [],
                     data=data or {}, lcl=lcl)


def f_strftime(dt, fmt='%Y-%m-%d %H:%M:%S'):
    if not dt:
        return ''
    return dt.strftime(fmt.encode('utf-8')).decode('utf-8')
