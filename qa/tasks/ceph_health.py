"""
ceph health check from ceph.log
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil

log = logging.getLogger(__name__)

@contextlib.contextmanager
def scan_for_unhealthy_logs(ctx, config):
    """
    Scan ceph.log for "HEALTH_ERR", "HEALTH_WARN" and "Health check failed"
    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)

    try:
        yield
    finally:
        log.info('Scanning ceph.log for unhealthy logs...')
        procs = list()
        for mon, _ in config.items():
            (remote,) = ctx.cluster.only(mon).remotes.keys()
            proc = remote.run(
                args=[
                    'sudo',
                    'egrep',
                    'HEALTH_WARN|HEALTH_ERR|Health check failed',
                    '/var/log/ceph/ceph.log',
                ],
                wait=False,
                check_status=False,
            )
            procs.append(proc)

        for proc in procs:
            proc.wait()
            if proc.returncode == 1: # 1 means no matches
                continue
            log.error('cluster gets unhealthy due to a test')
            raise Exception('cluster gets unhealthy due to a test')

@contextlib.contextmanager
def task(ctx, config):
    """
    For example, to run rgw on all monitors::

        tasks:
        - ceph-health:

    To only run on certain monitors::

        tasks:
        - ceph-health: [mon.0, mon.3]

    or

        tasks:
        - ceph-health:
            mon.0:
            mon.3:
    """
    if config is None:
        config = dict(('mon.{id}'.format(id=id_), None)
                      for id_ in teuthology.all_roles_of_type(
                          ctx.cluster, 'mon'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    mons = config.keys() # http://tracker.ceph.com/issues/20417

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph-health', {}))

    log.debug("config is {}".format(config))
    log.debug("mon list is {}".format(mons))

    with contextutil.nested(
        lambda: scan_for_unhealthy_logs(ctx=ctx, config=config),
        ):
        pass
    yield
