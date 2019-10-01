import os

BASE_DIR = os.path.dirname(__file__)

INSTALLED_APPS = (
    'gcloudc',
    'gcloudc.commands',
    'gcloudc.tests'
)

DATABASES = {
    'default': {
        'ENGINE': 'gcloudc.db.backends.datastore',
        'INDEXES_FILE': os.path.join(os.path.abspath(os.path.dirname(__file__)), "djangaeidx.yaml"),
        "PROJECT": "test"
    }
}

SECRET_KEY = "secret_key_for_testing"
