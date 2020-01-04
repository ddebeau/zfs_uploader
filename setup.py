from setuptools import setup

setup(
    name='zfs_uploader',
    version='0.1.0',
    packages=['zfs_uploader'],
    url='',
    license='MIT',
    author='David Debeau',
    author_email='ddebeau@gmail.com',
    description='ZFS snapshot to blob storage uploader. ',
    entry_points={
        'console_scripts': [
            'zfs_uploader = zfs_uploader.__main__:main',
        ]},
    install_requires=['apscheduler', 'boto3']
)
