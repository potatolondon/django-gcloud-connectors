import tempfile

from . import (
    CloudDatastoreRunner,
    locate_command,
)

BaseCommand = locate_command("test")


class Command(CloudDatastoreRunner, BaseCommand):
    USE_MEMORY_DATASTORE_BY_DEFAULT = True

    def _datastore_filename(self):
        print("Creating temporary test database...")

        # This returns the path to a temporary directory which the cloud
        # datastore emulator then intializes as a new
        # datastore
        return tempfile.TemporaryDirectory().name
