import argparse
import logging
from logging.handlers import RotatingFileHandler
import sys

from apscheduler.schedulers.background import BlockingScheduler

from zfs_uploader import __version__
from zfs_uploader.config import Config

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def main():
    parser = argparse.ArgumentParser(
        description='ZFS snapshot to blob storage uploader.')
    parser.add_argument('--log',
                        default='zfs_uploader.log',
                        help='Log file location. Defaults to '
                             '\'zfs_uploader.log\'')
    parser.add_argument('-v', '--version',
                        action='store_true',
                        help='Display software version.')
    args = parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    logger = logging.getLogger('zfs_uploader')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)

    fh = RotatingFileHandler(args.log, maxBytes=5*1024*1024, backupCount=5)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    config = Config()
    scheduler = BlockingScheduler()

    for job in config.jobs.values():
        logger.info(f'Adding job {job.filesystem}')
        scheduler.add_job(job.start, 'cron', **job.cron,
                          misfire_grace_time=2*60*60,
                          coalesce=True)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
