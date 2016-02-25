import cgi
import urllib2
import functools
import jinja2
import json
import calendar
from datetime import datetime
from UserDict import UserDict


def escape_result(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not kwargs.get('escape_result', True):
            del kwargs['escape_result']
            return f(*args, **kwargs)
        t = f(*args, **kwargs)
        if t is None:
            return None
        if not isinstance(t, (str, unicode)):
            return '**'
        return cgi.escape(t, quote=True)
    return wrapper


def _string_func(f):
    @escape_result
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except (LookupError, AttributeError, TypeError, ValueError):
            return '--'
    return wrapper

_env = jinja2.Environment(extensions=[
    'jinja2.ext.autoescape', 'jinja2.ext.loopcontrols', 'jinja2.ext.do',
], loader=jinja2.FileSystemLoader('templates'))


def _filter(f):
    _env.filters[f.__name__[2:]] = f
    return f


def _global(f):
    _env.globals[f.__name__[2:]] = f
    return f


@_filter
def f_tojson(obj):
    def default(obj):
        if isinstance(obj, datetime):
            return long(1000 * calendar.timegm(obj.timetuple()))
        if isinstance(obj, UserDict):
            return obj.data
        return obj
    return json.dumps(obj, default=default).replace(
        '<', u'\\u003c').replace('>', u'\\u003e').replace(
            '&', u'\\u0026').replace("'", u'\\u0027')


@_filter
def f_urlencode(text):
    return urllib2.quote(text.encode('utf8')).replace('/', '%2F')


@_filter
@_string_func
def f_strftime(dt, fmt='%Y-%m-%d %H:%M:%S'):
    if not dt:
        return ''
    return dt.strftime(fmt.encode('utf-8')).decode('utf-8')


@_filter
def f_render_node(node, level, **kwargs):
    return render('components/node/%s.html' % level, node=node, **kwargs)


@_filter
def f_render_cluster(cluster, level, **kwargs):
    return render('components/cluster/%s.html' % level, cluster=cluster,
                  **kwargs)


@_filter
def f_render_proxy(proxy, level, **kwargs):
    return render('components/proxy/%s.html' % level, proxy=proxy, **kwargs)


def component(tp, **kwargs):
    return render('components/%s.html' % tp, **kwargs)


@_global
def g_icon(icon, color=None):
    return component('icon', icon=icon, color=color)


@_global
def g_label(text, size=2, offset=0, id=None, cls=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'lbl-' + id
    return component('label', text=text, size=size, offset=offset, id=id,
                     cls=cls or [], data=data or {}, lcl=lcl)


@_global
def g_hint(text, size=2, offset=0, id=None, cls=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'lbl-' + id
    return component('hint', text=text, size=size, offset=offset, id=id,
                     cls=cls or [], data=data or {}, lcl=lcl)


@_global
def g_input(size=2, offset=0, id=None, cls=None, value=None, placeholder=None,
            addon=None, readonly=False, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'input-' + id
    return component('input', size=size, offset=offset, id=id, cls=cls or [],
                     value=value or '', placeholder=placeholder or '',
                     addon=addon, readonly=readonly, data=data or {})


@_global
def g_button(text, size=2, offset=0, color='default', id=None, cls=None,
             icon=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'button-' + id
    return component('button', text=text, size=size, offset=offset, id=id,
                     color=color, cls=cls or [], icon=icon, data=data or {},
                     lcl=lcl)


@_global
def g_checkbox(text, size=2, offset=0, color='default', checked=False,
               id=None, cls=None, data=None, lcl=None):
    if lcl is None and id is not None:
        lcl = 'checkbox-' + id
    return component('checkbox', text=text, size=size, offset=offset,
                     color=color, checked=checked, id=id, cls=cls or [],
                     data=data or {}, lcl=lcl)


def render(filename, **kwargs):
    return _env.get_template(filename).render(render_template=render, **kwargs)
