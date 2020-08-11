import sleuth
from . import TestCase
from .models import TestUserTwo
from django.conf import settings


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
