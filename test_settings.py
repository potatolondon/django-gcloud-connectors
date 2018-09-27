import os

INSTALLED_APPS = (
    'gcloudc',
)

DATABASES = {
    'default': {
        'ENGINE': 'gcloudc.db.backends.datastore',
        'INDEXES_FILE': os.path.join(os.path.abspath(os.path.dirname(__file__)), "djangaeidx.yaml")
    }
}

SECRET_KEY = "secret_key_for_testing"
