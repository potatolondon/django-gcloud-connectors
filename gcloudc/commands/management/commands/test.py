from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management import load_command_class

from . import CloudDatastoreRunner


def locate_test():
    try:
        index = settings.INSTALLED_APPS.index('gcloudc.commands')
    except ValueError:
        raise ImproperlyConfigured("Unable to locate gcloudc.commands in INSTALLED_APPS")

    APPS_TO_CHECK = list(settings.INSTALLED_APPS) + ['django.core']

    for i in range(index + 1, len(APPS_TO_CHECK)):
        app_label = APPS_TO_CHECK[i]
        command = load_command_class(app_label, 'test')
        if command:
            return command.__class__
    else:
        raise ImportError("Unable to locate a base test Command to subclass")


BaseCommand = locate_test()


class Command(CloudDatastoreRunner, BaseCommand):
    pass
