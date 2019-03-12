import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

DESC = """
    Django Database Backends for Google Cloud Databases
"""

setup(
    name='django-gcloud-connectors',
    version='0.1',
    packages=find_packages(),
    description='Django Database Backends for Google Cloud Databases',
    long_description=DESC,
    author='Potato London Ltd.',
    author_email='mail@p.ota.to',
    url='https://github.com/potatolondon/django-gcloud-connectors/',
    license='MIT',
    install_requires=[
        'Django>=2.0,<3.0',
        'pyyaml',
        'google-cloud-datastore',
        'sleuth-mock==0.1'
    ]
)
