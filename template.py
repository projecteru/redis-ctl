import os
import cgi
import urllib2
import functools
import jinja2
import json
import calendar
from datetime import datetime
from UserDict import UserDict

import config


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
def f_render_page(page, level):
    return render('page/component/' + level + '.html', page=page,
                  page_uri='/p/' + page.full_uri)


@_filter
def f_render_comment(comment, level='default'):
    return render('comment/component/' + level + '.html', comment=comment)


@_filter
def f_render_user(user, level='default'):
    return render('user/component/' + level + '.html', user=user)


@_filter
def f_render_mathcom(node, level='default'):
    path = 'mathcom/{tp}/{lvl}.html'.format(tp=node['type'], lvl=level)
    if not os.path.isfile('./templates/' + path):
        path = 'mathcom/{tp}/default.html'.format(tp=node['type'])
    return render(path, node=node)


@_filter
def f_markdown(text):
    import markdown.document
    if not isinstance(text, unicode):
        return text
    return markdown.document.compile_entire(text)


@_filter
def f_markdown_trunc(text, limit):
    import markdown.document
    if not isinstance(text, unicode):
        return text
    return markdown.document.compile_partial(text, limit)[0]


@_filter
def f_markdown_line(text):
    from markdown.inline import forge as inline_forge
    if not isinstance(text, unicode):
        return text
    return inline_forge(text)


@_filter
def f_urlencode(text):
    return urllib2.quote(text.encode('utf8')).replace('/', '%2F')


@_filter
def f_encode_anchor(text):
    return f_urlencode(text).replace('%', '.')


@_filter
@_string_func
def f_strftime(dt, fmt='%Y-%m-%d %H:%M:%S'):
    if not dt:
        return ''
    return dt.strftime(fmt.encode('utf-8')).decode('utf-8')


@_filter
def f_iter(x):
    if isinstance(x, dict):
        return x.iteritems()
    try:
        return iter(x)
    except TypeError:
        return []


def render(filename, **kwargs):
    return _env.get_template(filename).render(**kwargs)
