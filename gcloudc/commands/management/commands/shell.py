from . import CloudDatastoreRunner, locate_command


BaseCommand = locate_command("shell")


class Command(CloudDatastoreRunner, BaseCommand):
    pass
