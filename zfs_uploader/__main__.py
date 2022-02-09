import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import click
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BlockingScheduler

from zfs_uploader import __version__
from zfs_uploader.config import Config

LOG_FORMAT = 'time=%(asctime)s.%(msecs)03d level=%(levelname)s %(message)s'


@click.group()
@click.option('--config-path', default='config.cfg',
              help='Config file path.',
              show_default=True)
@click.option('--log-path', default='zfs_uploader.log',
              help='Log file path.',
              show_default=True)
@click.pass_context
def cli(ctx, config_path, log_path):
    logger = logging.getLogger('zfs_uploader')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%dT%H:%M:%S')

    fh = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=5)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if not os.path.isfile(config_path):
        print('No configuration file found.')
        sys.exit(1)

    ctx.obj = {
        'config_path': config_path,
        'logger': logger
    }


@cli.command()
@click.pass_context
def backup(ctx):
    """ Start backup job scheduler. """
    config_path = ctx.obj['config_path']
    logger = ctx.obj['logger']

    config = Config(config_path)
    scheduler = BlockingScheduler(
        executors={'default': ThreadPoolExecutor(max_workers=1)},
        job_defaults={'misfire_grace_time': None}
    )

    for job in config.jobs.values():
        logger.info(f'filesystem={job.filesystem} '
                    f'cron="{job.cron}" '
                    'msg="Adding job."')
        scheduler.add_job(job.start, 'cron', **job.cron, coalesce=True)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


@cli.command('list')
@click.argument('filesystem', required=False)
@click.pass_context
def list_backups(ctx, filesystem):
    """ List backups. """
    config_path = ctx.obj['config_path']
    logger = ctx.obj['logger']

    logger.setLevel('CRITICAL')

    config = Config(config_path)

    if filesystem:
        job = config.jobs.get(filesystem)
        jobs = {filesystem: job}

        if job is None:
            print('Filesystem does not exist in config file.')
            sys.exit(1)

    else:
        jobs = config.jobs

        if jobs is None:
            print('No filesystems exist in config file.')
            sys.exit(1)

    for filesystem, job in jobs.items():
        print(f'{filesystem}:\n')
        print('{0:<16} {1:<16} {2:<5} {3:<14}'.format('time', 'dependency',
                                                      'type', 'size (bytes)'))
        print('-'*52)
        for b in job.backup_db.get_backups():
            dependency = b.dependency or str(b.dependency)
            backup_size = b.backup_size or str(b.backup_size)
            print(f'{b.backup_time:<16} {dependency:<16} '
                  f'{b.backup_type:<5} {backup_size:<14}')
        print('\n')


@cli.command()
@click.option('--destination', help='Destination filesystem.')
@click.argument('filesystem')
@click.argument('backup-time', required=False)
@click.pass_context
def restore(ctx, destination, filesystem, backup_time):
    """ Restore from backup.

    Defaults to most recent backup if backup-time is not specified.

    WARNING: If restoring to a file system that already exists, snapshots
    and data that were written after the backup will be destroyed. Set
    `destination` in order to restore to a new file system.

    """
    config_path = ctx.obj['config_path']

    config = Config(config_path)
    job = config.jobs.get(filesystem)

    if job is None:
        print('Filesystem does not exist.')
        sys.exit(1)

    job.restore(backup_time, destination)

    print('Restore successful.')


@cli.command(help='Print version.')
def version():
    print(__version__)


if __name__ == '__main__':
    cli()
