from . import TestCase

from django.db import models


class TestUser(models.Model):
    """Basic model defintion for use in test cases."""
    username = models.CharField(max_length=32)
    email = models.EmailField(primary_key=True)

    def __unicode__(self):
        return self.username

    class Meta:
        app_label = "djangae"
        unique_together = ('username', 'email')


class TestUserWithUnique(models.Model):
    """Basic model with unique constaints enabled."""
    username = models.CharField(max_length=32)
    email = models.EmailField()

    def __unicode__(self):
        return self.username

    class Meta:
        app_label = "djangae"
        unique_together = ('username', 'email')


class DeleteTestCase(TestCase):

    def test_entity_deleted(self):
        """Testing the basic `delete()` ORM interaction."""

        user_one = TestUser.objects.create(
            username="A",
            email="a-user-a@test.com"
        )

        # FIXME-GCG when the test database is reset per unit test
        # user_count = TestUser.objects.count()
        # self.assertEqual(user_count, 1)

        # now delete the entity
        user_one.delete()
        
        # attempting to refresh using the in memory reference should
        # raise a DoesNotExist exception
        with self.assertRaises(TestUser.DoesNotExist):
            user_one.refresh_from_db()

        # explicitly fetching by the old username too
        # FIXME-GCG - this get will always fail until we clear the test db on each run
        # with self.assertRaises(TestUser.DoesNotExist):
        #     TestUser.objects.get(username='A')

        # # FIXME-GCG the count should also be decremented from 1 to 0
        # user_count = TestUser.objects.count()
        # self.assertEqual(user_count, 0)

    def test_unique_markers_deleted(self):
        """FIXME-GCG"""
        pass

    def test_cache_keys_deleted(self):
        """FIXME-GCG"""
        pass

    # we probably want to skip this test by default once it passes...
    def test_500_operations_limit(self):
        """FIXME-GCG"""
        pass

    def test_bulk_delete(self):
        """FIXME-GCG"""
        pass
