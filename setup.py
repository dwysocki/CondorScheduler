"""CondorScheduler: A tool for generating and submitting simple HTCondor DAG files for batch processing.

CondorScheduler is a tool for generating and submitting simple HTCondor
DAG files for batch processing.
"""

DOCLINES = __doc__.split("\n")

CLASSIFIERS = """\
Programming Language :: Python
Programming Language :: Python :: 3
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: MIT License
Operating System :: OS Independent
"""

MAJOR       = 0
MINOR       = 1
MICRO       = 0
__version__ = '%d.%d.%d' % (MAJOR, MINOR, MICRO)

def setup_package():
    metadata = dict(
        name='CondorScheduler',
        url='https://github.com/dwysocki/CondorScheduler',
        description=DOCLINES[0],
        long_description="\n".join(DOCLINES[2:]),
        version=__version__,
        package_dir={'': 'src'},
        packages=['CondorScheduler'],
        entry_points={
            'console_scripts': [
                'CondorScheduler = CondorScheduler.cli:main'
            ]
        },
        keywords=[
            'HTCondor',
            'batch processing'
        ],
        classifiers=[f for f in CLASSIFIERS.split('\n') if f]
    )

    from setuptools import setup

    setup(**metadata)

if __name__ == '__main__':
    setup_package()
