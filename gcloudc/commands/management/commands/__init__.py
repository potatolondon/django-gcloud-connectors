from datetime import datetime
import os
import logging
import time
import subprocess
import json
from django.core.exceptions import ImproperlyConfigured
from django.conf import settings
from urllib.request import urlopen
from urllib.error import HTTPError, URLError


_COMPONENTS_LIST_COMMAND = "gcloud components list --format=json".split()
_REQUIRED_COMPONENTS = set(['beta', 'cloud-datastore-emulator', 'core'])

_BASE_COMMAND = "gcloud beta emulators datastore start --quiet --project=test".split()
_DEFAULT_PORT = 9090


class CloudDatastoreRunner:
    def __init__(self, *args, **kwargs):
        self._process = None
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--nodatastore", action="store_false", dest="datastore", default=True)
        parser.add_argument("--datastore-port", action="store", dest="port", default=_DEFAULT_PORT)

    def execute(self, *args, **kwargs):
        try:
            if kwargs.get("datastore", True):
                self._check_gcloud_components()
                self._start_emulator(**kwargs)

            super().execute(*args, **kwargs)
        finally:
            self._stop_emulator()

    def _check_gcloud_components(self):
        finished_process = subprocess.run(_COMPONENTS_LIST_COMMAND, stdout=subprocess.PIPE, encoding='utf-8')
        installed_components = \
            set([cp['id'] for cp in json.loads(finished_process.stdout) if cp['current_version_string'] is not None])

        if not _REQUIRED_COMPONENTS.issubset(installed_components):
            raise RuntimeError(
                "Missing Google Cloud SDK component(s): {}\n"
                "Please run `gcloud components install` to install missing components.".format(
                    ", ".join(_REQUIRED_COMPONENTS - installed_components)
                )
            )

    def _get_args(self, **kwargs):
        BASE_DIR = getattr(settings, "BASE_DIR", None)

        if not BASE_DIR:
            raise ImproperlyConfigured("Please define BASE_DIR in your Django settings")

        return [
            "--data-dir=%s" % (os.path.join(BASE_DIR, ".datastore")),
            "--host-port=127.0.0.1:%s" % kwargs.get("port", _DEFAULT_PORT)
        ]

    def _wait_for_datastore(self):
        TIMEOUT = 60.0

        start = datetime.now()

        print("Waiting for Cloud Datastore Emulator...")
        time.sleep(1)

        while True:
            try:
                response = urlopen("http://127.0.0.1:%s/" % _DEFAULT_PORT)
            except (HTTPError, URLError):
                time.sleep(3)
                logging.exception(
                    "Error connecting to the Cloud Datastore Emulator. Retrying..."
                )
                continue

            if response.status == 200:
                break

            if (datetime.now() - start).total_seconds() > TIMEOUT:
                raise RuntimeError(
                    "Unable to start Cloud Datastore Emulator. Please check the logs."
                )

            time.sleep(1)

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
