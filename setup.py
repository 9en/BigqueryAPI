# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='BigqueryAPI',
    version='1.0.0',
    description='bigquery data control',
    long_description=readme,
    author='9en',
    author_email='mty.0613@gmail.com',
    python_requires='>=3.4',
    install_requires=['configparser', 'google-cloud-bigquery', 'importlib'],
    url='https://github.com/9en/BigqueryAPI',
    license=license,
    packages=find_packages()
)

