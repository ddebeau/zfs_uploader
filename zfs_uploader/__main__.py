from apscheduler.schedulers.background import BlockingScheduler

from zfs_uploader.config import Config


def main():
    config = Config()
    scheduler = BlockingScheduler()

    for job in config.jobs.values():
        scheduler.add_job(job.start, 'cron', **job.cron)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
