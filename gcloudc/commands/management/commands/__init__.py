import json
import logging
import os
import subprocess
import time
from datetime import datetime
from urllib.error import (
    HTTPError,
    URLError,
)
from urllib.request import urlopen

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management import load_command_class
from django.utils.autoreload import DJANGO_AUTORELOAD_ENV

_COMPONENTS_LIST_COMMAND = "gcloud components list --format=json".split()
_REQUIRED_COMPONENTS = set(["beta", "cloud-datastore-emulator", "core"])

_BASE_COMMAND = "gcloud beta emulators datastore start --user-output-enabled=false --consistency=1.0 --quiet --project=test".split()  # noqa
_DEFAULT_PORT = 9090

logger = logging.getLogger(__name__)


class CloudDatastoreRunner:
    USE_MEMORY_DATASTORE_BY_DEFAULT = False

    def __init__(self, *args, **kwargs):
        self._process = None
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--no-datastore", action="store_false", dest="datastore", default=True)
        parser.add_argument("--datastore-port", action="store", dest="port", default=_DEFAULT_PORT)
        parser.add_argument(
            "--use-memory-datastore",
            action="store_true",
            dest="use_memory_datastore",
            default=self.USE_MEMORY_DATASTORE_BY_DEFAULT,
        )

    def execute(self, *args, **kwargs):
        try:
            if kwargs.get("datastore", True) and os.environ.get(DJANGO_AUTORELOAD_ENV) != "true":
                self._check_gcloud_components()
                self._start_emulator(**kwargs)

            super().execute(*args, **kwargs)
        finally:
            self._stop_emulator()

    def _check_gcloud_components(self):
        finished_process = subprocess.run(_COMPONENTS_LIST_COMMAND, stdout=subprocess.PIPE, encoding="utf-8")
        installed_components = set(
            [cp["id"] for cp in json.loads(finished_process.stdout) if cp["current_version_string"] is not None]
        )

        if not _REQUIRED_COMPONENTS.issubset(installed_components):
            raise RuntimeError(
                "Missing Google Cloud SDK component(s): {}\n"
                "Please run `gcloud components install` to install missing components.".format(
                    ", ".join(_REQUIRED_COMPONENTS - installed_components)
                )
            )

    def _datastore_filename(self):
        BASE_DIR = getattr(settings, "BASE_DIR", None)

        if not BASE_DIR:
            raise ImproperlyConfigured("Please define BASE_DIR in your Django settings")

        return os.path.join(BASE_DIR, ".datastore")

    def _get_args(self, **kwargs):
        args = ["--host-port=127.0.0.1:%s" % kwargs.get("port", _DEFAULT_PORT)]

        if kwargs["use_memory_datastore"]:
            logger.info("Using in-memory datastore")
            args.append("--no-store-on-disk")
        else:
            args.append("--data-dir=%s" % self._datastore_filename())

        return args

    def _wait_for_datastore(self, **kwargs):
        TIMEOUT = 60.0

        start = datetime.now()

        logger.info("Waiting for Cloud Datastore Emulator...")
        time.sleep(1)

        failures = 0
        while True:
            try:
                response = urlopen("http://127.0.0.1:%s/" % kwargs["port"])
            except (HTTPError, URLError):
                failures += 1
                time.sleep(1)
                if failures > 5:
                    # Only start logging if this becomes persistent
                    logger.exception(
                        "Error connecting to the Cloud Datastore Emulator. Retrying...")
                continue

            if response.status == 200:
                # Give things a second to really boot
                time.sleep(2)
                break

            if (datetime.now() - start).total_seconds() > TIMEOUT:
                raise RuntimeError("Unable to start Cloud Datastore Emulator. Please check the logs.")

            time.sleep(1)

    def _start_emulator(self, **kwargs):
        logger.info("Starting Cloud Datastore Emulator")

        os.environ["DATASTORE_EMULATOR_HOST"] = "127.0.0.1:%s" % kwargs["port"]
        os.environ["DATASTORE_PROJECT_ID"] = "test"

        # The Cloud Datastore emulator regularly runs out of heap space
        # so set a higher max
        os.environ["JDK_JAVA_OPTIONS"] = "-Xms512M -Xmx1024M"

        env = os.environ.copy()
        self._process = subprocess.Popen(_BASE_COMMAND + self._get_args(**kwargs), env=env)

        self._wait_for_datastore(**kwargs)

    def _stop_emulator(self):
        logger.info("Stopping Cloud Datastore Emulator")
        if self._process:
            self._process.terminate()
            self._process.wait()
            self._process = None


def locate_command(name):
    """
        Apps may override Django commands, what we want to do is
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
            command = load_command_class(app_label, name)
        except ModuleNotFoundError:
            continue

        if command:
            return command.__class__
    else:
        raise ImportError("Unable to locate a base %s Command to subclass" % name)
