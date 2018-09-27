import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='django-gcloud-connectors',
    version='0.1',
    packages=['gcloudc'],
    description='Django Database Backends for Google Cloud Databases',
    long_description=README,
    author='Potato London Ltd.',
    author_email='mail@p.ota.to',
    url='https://github.com/potatolondon/django-gcloud-connectors/',
    license='MIT',
    install_requires=[
        'Django>=2.0,<3.0',
        'google-cloud-datastore'
    ]
)
