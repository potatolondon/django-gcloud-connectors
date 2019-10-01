import json
from . import TestCase
from unittest.mock import patch
from gcloudc.commands.management.commands import _REQUIRED_COMPONENTS, CloudDatastoreRunner


class CloudDatastoreRunnerTest(TestCase):
    def test_check_gcloud_components(self):
        class MockProcess:
            stdout = json.dumps([{"id": cp, "current_version_string": "0.1"} for cp in list(_REQUIRED_COMPONENTS)[:-1]])

        # We mock _start_emulator as we don't want to get that far in execute()
        with patch(
            "gcloudc.commands.management.commands.CloudDatastoreRunner._start_emulator",
            side_effect=AssertionError("Google Cloud components check failed"),
        ):

            with patch("gcloudc.commands.management.commands.subprocess.run", return_value=MockProcess()):
                command = CloudDatastoreRunner()
                with self.assertRaises(RuntimeError):
                    command.execute()
