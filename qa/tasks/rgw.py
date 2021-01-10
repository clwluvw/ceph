"""
rgw routines
"""
import argparse
import contextlib
import logging

from teuthology.orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.exceptions import ConfigError
from tasks.util import get_remote_for_role

log = logging.getLogger(__name__)

class RGWEndpoint:
    def __init__(self, hostname=None, port=None, use_https=False, dns_name=None, website_dns_name=None):
        self.hostname = hostname
        self.port = port
        self.use_https = use_https
        self.dns_name = dns_name
        self.website_dns_name = website_dns_name

    def url(self):
        proto = 'https' if self.use_https else 'http'
        return '{proto}://{hostname}:{port}/'.format(proto=proto, hostname=self.hostname, port=self.port)

def assign_endpoints(ctx, config, use_https):
    role_endpoints = {}
    for role, client_config in config.items():
        client_config = client_config or {}
        remote = get_remote_for_role(ctx, role)

        port = client_config.get('port', 443 if use_https else 80)

        # if dns-name is given, use it as the hostname (or as a prefix)
        dns_name = client_config.get('dns-name', '')
        if len(dns_name) == 0 or dns_name.endswith('.'):
            dns_name += remote.hostname

        website_dns_name = client_config.get('dns-s3website-name')
        if website_dns_name is not None and (len(website_dns_name) == 0 or website_dns_name.endswith('.')):
            website_dns_name += remote.hostname

        role_endpoints[role] = RGWEndpoint(remote.hostname, port, use_https, dns_name, website_dns_name)

    return role_endpoints

@contextlib.contextmanager
def task(ctx, config):
    """
    For example, to run rgw on all clients::

        tasks:
        - ceph:
        - rgw:

    To only run on certain clients::

        tasks:
        - ceph:
        - rgw: [client.0, client.3]

    or

        tasks:
        - ceph:
        - rgw:
            client.0:
            client.3:

    To run radosgw through valgrind:

        tasks:
        - ceph:
        - rgw:
            client.0:
              valgrind: [--tool=memcheck]
            client.3:
              valgrind: [--tool=memcheck]

    To configure data or index pool pg_size:

        overrides:
          rgw:
            data_pool_pg_size: 256
            index_pool_pg_size: 128
    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                      for id_ in teuthology.all_roles_of_type(
                          ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    clients = config.keys() # http://tracker.ceph.com/issues/20417

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('rgw', {}))

    ctx.rgw = argparse.Namespace()

    use_https = config.pop('use_https', False)
    ctx.rgw.config = config

    log.debug("config is {}".format(config))
    log.debug("client list is {}".format(clients))

    ctx.rgw.role_endpoints = assign_endpoints(ctx, config, use_https)
