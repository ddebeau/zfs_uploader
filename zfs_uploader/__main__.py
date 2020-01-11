import logging
from logging.handlers import RotatingFileHandler
import sys

from apscheduler.schedulers.background import BlockingScheduler

from zfs_uploader.config import Config

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def main():
    logger = logging.getLogger('zfs_uploader')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)

    fh = RotatingFileHandler('zfs_uploader.log', maxBytes=5*1024*1024,
                             backupCount=5)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    config = Config()
    scheduler = BlockingScheduler()

    for job in config.jobs.values():
        logger.info(f'Adding job {job.filesystem}')
        scheduler.add_job(job.start, 'cron', **job.cron)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
