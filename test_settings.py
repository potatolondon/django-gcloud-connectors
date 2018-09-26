INSTALLED_APPS = (
    'gcloudc',
)

DATABASES = {
    'default': {
        'ENGINE': 'gcloudc.db.backends.datastore',
    }
}

SECRET_KEY = "secret_key_for_testing"
