""" Setup script for the pypgsync application.

"""
from setuptools import setup, find_packages

setup(
    name='pypgsync',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click==7.0',
        'sqlalchemy==1.3.0',
        'psycopg2-binary==2.7.7',
        'sqlalchemy-utils==0.33.11',
        'crayons==0.2.0',
        'vistir[spinner]==0.3.1',
    ],
    entry_points='''
        [console_scripts]
        pypgsync=pypgsync.cli:cli
    ''',
)
