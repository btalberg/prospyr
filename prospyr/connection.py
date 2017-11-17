# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, unicode_literals

from datetime import datetime
import functools
import re
import threading
import time

import requests
from urlobject import URLObject
from urlobject.path import URLPath

from prospyr.cache import InMemoryCache
from prospyr.exceptions import MisconfiguredError
from prospyr.util import seconds

_connections = {}
_default_url = 'https://api.prosperworks.com/developer_api/'


def connect(email, token, url=_default_url, name='default', cache=None):
    """
    Create a connection to ProsperWorks using credentials `email` and `token`.

    The new connection is returned. It can be retrieved later by name using
    `prospyr.connection.get`. By default the connection is named 'default'. You
    can provide a different name to maintain multiple connections to
    ProsperWorks.

    By default an in-memory URL cache is used. Argue
    cache=prospyr.cache.NoOpCache() to disable caching.
    """
    if name in _connections:
        existing = _connections[name]
        raise ValueError(
            '`{name}` is already connected using account '
            '"{email}"'.format(name=name, email=existing.email)
        )

    validate_url(url)

    conn = Connection(url, email, token, cache=cache, name=name)
    _connections[name] = conn
    return conn


def get(name='default'):
    """
    Fetch a ProsperWorks connection by name.

    If you did not argue a name at connection time, the connection will be
    named 'default'.
    """
    try:
        return _connections[name]
    except KeyError:
        if name == 'default':
            msg = ('There is no default connection. '
                   'First try prospyr.connect(...)')
        else:
            msg = ('There is no connection named "{name}". '
                   'First try prospyr.connect(..., name="{name}")')
            msg = msg.format(name=name)
        raise MisconfiguredError(msg)


def validate_url(url):
    """
    True or MisconfiguredError if `url` is invalid.
    """
    uo = URLObject(url)
    if not uo.scheme or uo.scheme not in {'http', 'https'}:
        raise MisconfiguredError('ProsperWorks API URL `%s` must include a '
                                 'scheme (http, https)' % url)
    if not uo.hostname:
        raise MisconfiguredError('ProsperWorks API URL `%s` must include a '
                                 'hostname' % url)
    if re.search('/v\d', url):
        raise MisconfiguredError('ProsperWorks API URL `%s` should not '
                                 'include a "version" path segment' % url)

    return True


def url_join(base, *paths):
    """
    Append `paths` to `base`. Path resets on each absolute path.

    Like os.path.join, but for URLs.
    """
    if not hasattr(base, 'add_path'):
        base = URLObject(base)

    for path in paths:
        path = URLPath(path)
        base = base.add_path(path)
    return base


class Connection(object):

    TEN_MINUTES = 600

    def __init__(self, url, email, token, name='default', version='v1',
                 cache=None, rate_limit=600):
        self.session = Connection._get_session(email, token)
        self.email = email
        self.base_url = URLObject(url)
        self.api_url = self.base_url.add_path_segment(version)
        self.cache = InMemoryCache() if cache is None else cache
        self.name = name
        self.rate_limit = rate_limit
        self.last_api_call = None

    def http_method(self, method, url, *args, **kwargs):
        """
        Send HTTP request with `method` to `url`.
        """
        lock = threading.RLock()

        with lock:
            if self.rate_limit:
                if self.last_api_call:
                    elapsed = (datetime.now() - self.last_api_call).total_seconds()
                    if elapsed < 60 / self.rate_limit:
                        sleep = 60 / self.rate_limit - elapsed
                        # app.logger.debug("Enforcing rate limit by sleeping {0} elapsed".format(sleep))
                        time.sleep(sleep)
                # I decided to record the last api call time before the request, instead of after
                # this is conservative, because the time to make the request is not included
                # I considered timing if from after the super call returns, but there is some
                # time spent on processing the result and, consequently, if you make a call
                # immediately thereafter it /could/ be too quick
                self.last_api_call = datetime.now()

        method_fn = getattr(self.session, method)
        return method_fn(url, *args, **kwargs)

    def build_absolute_url(self, path):
        """
        Resolve relative `path` against this connection's API url.
        """
        return url_join(self.api_url, path)

    @staticmethod
    def _get_session(email, token):
        session = requests.Session()
        defaults = {
            'X-PW-Application': 'developer_api',
            'X-PW-AccessToken': token,
            'X-PW-UserEmail': email,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        session.headers.update(defaults)
        return session

    def __getattr__(self, name):
        """
        Turn HTTP verbs into http_method calls so e.g. conn.get(...) works.

        Note that 'get' and 'delete' are special-cased to handle caching
        """
        methods = 'post', 'put', 'patch', 'options'
        if name in methods:
            return functools.partial(self.http_method, name)
        return super(Connection, self).__getattr__(name)

    def get(self, url, *args, **kwargs):
        cached = self.cache.get(url)
        if cached is None:
            cached = self.http_method('get', url, *args, **kwargs)
            self.cache.set(url, cached, max_age=seconds(minutes=5))
        return cached

    def delete(self, url, *args, **kwargs):
        resp = self.http_method('delete', url, *args, **kwargs)
        if resp.ok:
            self.cache.clear(url)
        return resp
