from datetime import datetime
import os
import socket
import subprocess
from django.core.exceptions import ImproperlyConfigured
from django.conf import settings


_BASE_COMMAND = "gcloud beta emulators datastore start --quiet --project=test".split()


class CloudDatastoreRunner:
    def __init__(self, *args, **kwargs):
        self._process = None
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--nodatastore", action="store_false", dest="datastore", default=True)
        parser.add_argument("--datastore-port", action="store", dest="port", default=9090)

    def execute(self, *args, **kwargs):
        try:
            if kwargs.get("datastore", True):
                self._start_emulator(**kwargs)
            super().execute(*args, **kwargs)
        finally:
            self._stop_emulator()

    def _get_args(self, **kwargs):
        BASE_DIR = getattr(settings, "BASE_DIR", None)

        if not BASE_DIR:
            raise ImproperlyConfigured("Please define BASE_DIR in your Django settings")

        return [
            "--data-dir=%s" % (os.path.join(BASE_DIR, ".datastore")),
            "--host-port=127.0.0.1:%s" % kwargs.get("port", 9090)
        ]

    def _wait_for_datastore(self):
        TIMEOUT = 60.0

        start = datetime.now()

        print("Waiting for Cloud Datastore Emulator...")

        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', 8081))
            if result == 0:
                break

            if (datetime.now() - start).total_seconds() > TIMEOUT:
                raise RuntimeError(
                    "Unable to start Cloud Datastore Emulator. Please check the logs."
                )

    def _start_emulator(self, **kwargs):
        print("Starting Cloud Datastore Emulator")

        os.environ["DATASTORE_EMULATOR_HOST"] = "127.0.0.1:%s" % kwargs["port"]
        os.environ["DATASTORE_PROJECT_ID"] = "test"

        env = os.environ.copy()
        self._process = subprocess.Popen(
            _BASE_COMMAND + self._get_args(**kwargs),
            env=env
        )

        self._wait_for_datastore()

    def _stop_emulator(self):
        print("Stopping Cloud Datastore Emulator")
        if self._process:
            self._process.kill()
            self._process = None
