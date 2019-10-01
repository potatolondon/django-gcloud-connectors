from . import TestCase

from django.db import models


class TestUser(models.Model):
    """Basic model defintion for use in test cases."""

    username = models.CharField(max_length=32)

    def __unicode__(self):
        return self.username


class DeleteTestCase(TestCase):
    def test_entity_deleted(self):
        """Testing the basic `delete()` ORM interaction."""

        user_one = TestUser.objects.create(username="A")
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
        user_one = TestUser.objects.create(username="One")
        user_two = TestUser.objects.create(username="Two")
        user_three = TestUser.objects.create(username="Three")

        self.assertEqual(TestUser.objects.count(), 3)

        user_three = TestUser.objects.all().delete()
        self.assertEqual(TestUser.objects.count(), 0)
