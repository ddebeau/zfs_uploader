import logging
from logging.handlers import RotatingFileHandler
import sys

import click
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BlockingScheduler

from zfs_uploader import __version__
from zfs_uploader.config import Config

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


@click.group()
def cli():
    pass


@cli.command()
@click.option('--config-path', default='config.cfg',
              help='Config file path.',
              show_default=True)
@click.option('--log-path', default='zfs_uploader.log',
              help='Log file path.',
              show_default=True)
def backup(config_path, log_path):
    """ Start backup job scheduler. """
    logger = logging.getLogger('zfs_uploader')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)

    fh = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=5)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    config = Config(config_path)
    scheduler = BlockingScheduler(
        executors={'default': ThreadPoolExecutor(max_workers=1)}
    )

    for job in config.jobs.values():
        logger.info(f'Adding job {job.filesystem}')
        scheduler.add_job(job.start, 'cron', **job.cron,
                          misfire_grace_time=2*60*60,
                          coalesce=True)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


@cli.command('list')
@click.option('--config-path', default='config.cfg',
              help='Config file path.',
              show_default=True)
@click.argument('filesystem')
def list_backups(config_path, filesystem):
    """ List backups. """
    config = Config(config_path)
    job = config.jobs.get(filesystem)

    if job is None:
        print('Filesystem does not exist.')
        sys.exit(1)

    print('{0:<16} {1:<16} {2:<5}'.format('time', 'dependency', 'type'))
    print('-'*38)
    for b in job.backup_db.get_backups():
        dependency = b.dependency or str(b.dependency)
        print(f'{b.backup_time:<16} {dependency:<16} {b.backup_type:<5}')


@cli.command()
@click.option('--config-path', default='config.cfg',
              help='Config file path.',
              show_default=True)
@click.argument('filesystem')
@click.argument('backup-time', required=False)
def restore(config_path, filesystem, backup_time):
    """ Restore from backup.

    Defaults to most recent backup if backup_time is not specified.

    """
    config = Config(config_path)
    job = config.jobs.get(filesystem)

    if job is None:
        print('Filesystem does not exist.')
        sys.exit(1)

    job.restore(backup_time) if backup_time else job.restore()

    print('Restore successful.')


@cli.command(help='Print version.')
def version():
    print(__version__)


if __name__ == '__main__':
    cli()
