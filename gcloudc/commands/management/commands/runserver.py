from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management import load_command_class

from . import CloudDatastoreRunner


def locate_runserver():
    """
        Lots of apps override the runserver command, what we want to do is
        subclass whichever one had precedence before the gcloudc.commands app and subclass that
    """

    try:
        index = settings.INSTALLED_APPS.index("gcloudc.commands")
    except ValueError:
        raise ImproperlyConfigured("Unable to locate gcloudc.commands in INSTALLED_APPS")

    APPS_TO_CHECK = list(settings.INSTALLED_APPS) + ["django.core"]

    for i in range(index + 1, len(APPS_TO_CHECK)):
        app_label = APPS_TO_CHECK[i]
        try:
            command = load_command_class(app_label, "runserver")
        except ModuleNotFoundError:
            continue

        if command:
            return command.__class__
    else:
        raise ImportError("Unable to locate a base runserver Command to subclass")


BaseCommand = locate_runserver()


class Command(CloudDatastoreRunner, BaseCommand):
    pass
