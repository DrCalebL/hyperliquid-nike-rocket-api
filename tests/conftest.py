"""
Test isolation for the HL follower suite (QC security-6; the master's tests/harness.py does the same).

- Every outbound connection to a non-loopback host raises NetworkBlocked for the whole test process (the code under
  test can e-mail through Resend and some routes open psycopg2.connect(DATABASE_URL) directly). The host of
  TEST_DATABASE_URL stays reachable for the owner-run tests/test_billing_integration.py.
- Production variables are removed from the process before any app module is imported (several are read at import,
  e.g. order_utils.RESEND_API_KEY) and again for every test.
"""
import os
import socket
from urllib.parse import urlparse

import pytest

SCRUB_ENV = ('DATABASE_URL', 'RESEND_API_KEY', 'MASTER_API_KEY', 'ADMIN_PASSWORD', 'CREDENTIALS_ENCRYPTION_KEY',
             'GUARDIAN_MODE', 'HL_WEIGHT_CEILING', 'HL_GUARDIAN_WEIGHT_PER_MIN', 'USE_TESTNET', 'ADMIN_EMAIL',
             'COINBASE_COMMERCE_API_KEY', 'COINBASE_COMMERCE_WEBHOOK_SECRET')
SCRUB_PREFIXES = ('CHAOS_',)
_LOOPBACK = {'127.0.0.1', '::1', 'localhost', '0.0.0.0', ''}


class NetworkBlocked(OSError):
    """A test tried to reach a non-loopback host."""


def _scrub(environ):
    for name in list(environ):
        if name in SCRUB_ENV or name.startswith(SCRUB_PREFIXES):
            environ.pop(name, None)


def _allowed_hosts():
    hosts = set(_LOOPBACK)
    url = os.getenv('TEST_DATABASE_URL')
    if url:
        try:
            host = urlparse(url).hostname
        except ValueError:
            host = None
        if host:
            hosts.add(host)
    return frozenset(hosts)


def _host_of(address):
    if isinstance(address, (tuple, list)) and address:
        return str(address[0])
    return None


def _block_network():
    if getattr(socket, '_rocket_blocked', False):
        return
    allowed = _allowed_hosts()
    real_connect, real_connect_ex = socket.socket.connect, socket.socket.connect_ex
    real_create, real_gai = socket.create_connection, socket.getaddrinfo

    def guard(host, what):
        if host is not None and host not in allowed:
            raise NetworkBlocked('network blocked in tests: %s %s' % (what, host))

    def connect(sock, address):
        guard(_host_of(address), 'connect')
        return real_connect(sock, address)

    def connect_ex(sock, address):
        guard(_host_of(address), 'connect_ex')
        return real_connect_ex(sock, address)

    def create_connection(address, *args, **kwargs):
        guard(_host_of(address), 'create_connection')
        return real_create(address, *args, **kwargs)

    def getaddrinfo(host, *args, **kwargs):
        guard(None if host is None else str(host), 'getaddrinfo')
        return real_gai(host, *args, **kwargs)

    socket.socket.connect, socket.socket.connect_ex = connect, connect_ex
    socket.create_connection, socket.getaddrinfo = create_connection, getaddrinfo
    socket._rocket_blocked = True


_scrub(os.environ)
_block_network()


@pytest.fixture(autouse=True)
def _no_production_env(monkeypatch):
    for name in list(os.environ):
        if name in SCRUB_ENV or name.startswith(SCRUB_PREFIXES):
            monkeypatch.delenv(name, raising=False)
