from . import TestCase
from .models import TestUser


class DeleteTestCase(TestCase):
    def test_entity_deleted(self):
        """Testing the basic `delete()` ORM interaction."""

        user_one = TestUser.objects.create(username="A", first_name="A", second_name="B")
        self.assertEqual(TestUser.objects.count(), 1)

        user_one.delete()

        with self.assertRaises(TestUser.DoesNotExist):
            user_one.refresh_from_db()

        with self.assertRaises(TestUser.DoesNotExist):
            TestUser.objects.get(username="A")

        self.assertEqual(TestUser.objects.count(), 0)

    def test_cache_keys_deleted(self):
        """FIXME-GCG"""
        pass

    def test_bulk_delete(self):
        """Testing the basic `delete()` ORM interaction."""
        TestUser.objects.create(username="One", first_name="A", second_name="B")
        TestUser.objects.create(username="Two", first_name="B", second_name="B")
        TestUser.objects.create(username="Three", first_name="C", second_name="B")

        self.assertEqual(TestUser.objects.count(), 3)

        TestUser.objects.all().delete()
        self.assertEqual(TestUser.objects.count(), 0)
