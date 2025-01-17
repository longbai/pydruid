import io
import sys
from setuptools import setup

install_requires = [
    "six >= 1.9.0",
    "requests",
]

extras_require = {
    "pandas": ["pandas"],
    "async": ["tornado"],
    "sqlalchemy": ["sqlalchemy"],
    "cli": ["pygments", "prompt_toolkit<2.0.0", "tabulate"],
}

# only require simplejson on python < 2.6
if sys.version_info < (2, 6):
    install_requires.append("simplejson >= 3.3.0")

with io.open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pydruid-qiniu',
    version='0.999.0dev',
    author='Druid Developers',
    author_email='druid-development@googlegroups.com',
    packages=['pydruid', 'pydruid.db', 'pydruid.utils'],
    url='https://pypi.python.org/pypi/pydruid/',
    license='Apache License, Version 2.0',
    description='A Python connector for Druid.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=install_requires,
    extras_require=extras_require,
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'six', 'mock'],
    entry_points={
        'console_scripts': [
            'pydruid = pydruid.console:main',
        ],
        'sqlalchemy.dialects': [
            'druid = pydruid.db.sqlalchemy:DruidHTTPDialect',
            'druid.http = pydruid.db.sqlalchemy:DruidHTTPDialect',
            'druid.https = pydruid.db.sqlalchemy:DruidHTTPSDialect',
        ],
    },
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.6",
    ],
)
