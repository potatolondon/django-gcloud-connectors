import sleuth
from django.conf import settings
from django.core.management import call_command

from . import TestCase
from .models import TestUserTwo


class OperationsTests(TestCase):

    def test_bulk_batch_size_override(self):

        with sleuth.watch(
            "gcloudc.db.backends.datastore.commands.InsertCommand.__init__"
        ) as bbs:

            try:
                original = settings.DATABASES["default"].get("OPTIONS", {})
                settings.DATABASES["default"]["OPTIONS"] = {
                    'BULK_BATCH_SIZE': 25
                }

                users = [TestUserTwo(username=str(i)) for i in range(30)]
                TestUserTwo.objects.using("default").bulk_create(users)
            finally:
                settings.DATABASES["default"]["OPTIONS"] = original

            self.assertEqual(bbs.call_count, 2)
            self.assertEqual(len(bbs.calls[0].args[3]), 25)

    def test_flush_large_table(self):
        for i in range(502):
            TestUserTwo.objects.create(username=str(i))

        self.assertEqual(TestUserTwo.objects.count(), 502)
        call_command('flush', interactive=False)
        self.assertFalse(TestUserTwo.objects.exists())
