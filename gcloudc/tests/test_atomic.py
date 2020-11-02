
import threading

import sleuth
from django.db import connection
from gcloudc.db import transaction

from . import TestCase
from .models import (
    TestFruit,
    TestUser,
)


class TransactionTests(TestCase):
    def test_on_commit_works(self):
        def increment():
            increment.x += 1

        increment.x = 0

        with transaction.atomic():
            connection.on_commit(increment)
            self.assertEqual(increment.x, 0)

        self.assertEqual(increment.x, 1)

        try:
            with transaction.atomic():
                connection.on_commit(increment)
                self.assertEqual(increment.x, 1)
                raise ValueError()
        except ValueError:
            pass

        self.assertEqual(increment.x, 1)  # Still the same

        with transaction.atomic():
            pass  # commit hook should have gone with rollback

        self.assertEqual(increment.x, 1)  # Still the same

    def test_get_or_create(self):
        """
            get_or_create uses Django's atomic decorator under the hood
            this can cause issues if called within a gcloudc atomic block
        """

        with transaction.atomic():
            user, created = TestUser.objects.get_or_create(username="foo")
            self.assertTrue(created)

            user, created = TestUser.objects.get_or_create(username="foo")
            self.assertFalse(created)

    def test_repeated_usage_in_a_loop(self):
        pk = TestUser.objects.create(username="foo").pk
        for i in range(4):
            with transaction.atomic(xg=True):
                TestUser.objects.get(pk=pk)
                continue

        with transaction.atomic(xg=True):
            TestUser.objects.get(pk=pk)

    def test_recursive_atomic(self):
        lst = []

        @transaction.atomic
        def txn():
            lst.append(True)
            if len(lst) == 3:
                return
            else:
                txn()

        txn()

    def test_recursive_non_atomic(self):
        lst = []

        @transaction.non_atomic
        def txn():
            lst.append(True)
            if len(lst) == 3:
                return
            else:
                txn()

        txn()

    def test_atomic_in_separate_thread(self):
        """ Regression test.  See #668. """
        @transaction.atomic
        def txn():
            return

        def target():
            txn()

        thread = threading.Thread(target=target)
        thread.start()
        thread.join()

    def test_non_atomic_in_separate_thread(self):
        """ Regression test.  See #668. """
        @transaction.non_atomic
        def txn():
            return

        def target():
            txn()

        thread = threading.Thread(target=target)
        thread.start()
        thread.join()

    def test_atomic_decorator(self):
        @transaction.atomic
        def txn():
            TestUser.objects.create(username="foo", field2="bar")
            self.assertTrue(transaction.in_atomic_block())
            raise ValueError()

        with self.assertRaises(ValueError):
            txn()

        self.assertEqual(0, TestUser.objects.count())

    def test_atomic_context_manager(self):
        with self.assertRaises(ValueError):
            with transaction.atomic():
                TestUser.objects.create(username="foo", field2="bar")
                raise ValueError()

        self.assertEqual(0, TestUser.objects.count())

    def test_non_atomic_context_manager(self):
        existing = TestUser.objects.create(username="existing", field2="exists", first_name="one", second_name="one")

        with transaction.atomic():
            self.assertTrue(transaction.in_atomic_block())

            user = TestUser.objects.create(username="foo", field2="bar", first_name="two", second_name="two")

            with transaction.non_atomic():
                # We're outside the transaction, so the user should not exist
                self.assertRaises(TestUser.DoesNotExist, TestUser.objects.get, pk=user.pk)
                self.assertFalse(transaction.in_atomic_block())

                with sleuth.watch("google.cloud.datastore.client.Client.get") as datastore_get:
                    TestUser.objects.get(pk=existing.pk)  # Should hit the cache, not the datastore

                self.assertFalse(datastore_get.called)

            with transaction.atomic(independent=True):
                user2 = TestUser.objects.create(username="foo2", field2="bar2", first_name="three", second_name="three")
                self.assertTrue(transaction.in_atomic_block())

                with transaction.non_atomic():
                    self.assertFalse(transaction.in_atomic_block())
                    self.assertRaises(TestUser.DoesNotExist, TestUser.objects.get, pk=user2.pk)

                    with transaction.non_atomic():
                        self.assertFalse(transaction.in_atomic_block())
                        self.assertRaises(TestUser.DoesNotExist, TestUser.objects.get, pk=user2.pk)

                        with sleuth.watch("google.cloud.datastore.client.Client.get") as datastore_get:
                            # Should hit the cache, not the Datastore
                            TestUser.objects.get(pk=existing.pk)
                            self.assertFalse(datastore_get.called)

                    self.assertFalse(transaction.in_atomic_block())
                    self.assertRaises(TestUser.DoesNotExist, TestUser.objects.get, pk=user2.pk)

                # Should hit the cache
                self.assertTrue(TestUser.objects.filter(pk=user2.pk).exists())
                self.assertTrue(transaction.in_atomic_block())

        self.assertFalse(transaction.in_atomic_block())

    def test_independent_argument(self):
        """
            We would get a XG error if the inner transaction was not independent
        """
        @transaction.atomic
        def txn1(_username, _fruit):
            @transaction.atomic(independent=True)
            def txn2(_fruit):
                TestFruit.objects.create(name=_fruit, color="pink")
                raise ValueError()

            TestUser.objects.create(username=_username)
            txn2(_fruit)

        with self.assertRaises(ValueError):
            txn1("test", "banana")

    def test_enable_cache_argument(self):
        user = TestUser.objects.create(username="randy", first_name="Randy")

        with sleuth.watch('gcloudc.db.backends.datastore.context.CacheDict.get') as cachedict_get:
            TestUser.objects.get(username="randy")
            self.assertEqual(cachedict_get.call_count, 1)

            with transaction.atomic(enable_cache=False):
                user.first_name = "Łukasz"
                user.save()

                non_cached = TestUser.objects.get(username="randy")
                # Result is not fetched from the cache
                self.assertEqual(non_cached.first_name, "Randy")
                self.assertEqual(cachedict_get.call_count, 1)

    def test_nested_decorator(self):
        # Nested decorator pattern we discovered can cause a connection_stack
        # underflow.

        @transaction.atomic
        def inner_txn():
            pass

        @transaction.atomic
        def outer_txn():
            inner_txn()

        # Calling inner_txn first puts it in a state which means it doesn't
        # then behave properly in a nested transaction.
        inner_txn()
        outer_txn()


class TransactionStateTests(TestCase):

    def test_has_already_read(self):
        apple = TestFruit.objects.create(name="Apple", color="Red")
        pear = TestFruit.objects.create(name="Pear", color="Green")

        with transaction.atomic(xg=True) as txn:
            self.assertFalse(txn.has_already_been_read(apple))
            self.assertFalse(txn.has_already_been_read(pear))

            apple.refresh_from_db()

            self.assertTrue(txn.has_already_been_read(apple))
            self.assertFalse(txn.has_already_been_read(pear))

            with transaction.atomic(xg=True) as txn:
                self.assertTrue(txn.has_already_been_read(apple))
                self.assertFalse(txn.has_already_been_read(pear))
                pear.refresh_from_db()
                self.assertTrue(txn.has_already_been_read(pear))

                with transaction.atomic(independent=True) as txn2:
                    self.assertFalse(txn2.has_already_been_read(apple))
                    self.assertFalse(txn2.has_already_been_read(pear))

    def test_refresh_if_unread(self):
        apple = TestFruit.objects.create(name="Apple", color="Red")

        with transaction.atomic() as txn:
            apple.color = "Pink"

            txn.refresh_if_unread(apple)

            self.assertEqual(apple.name, "Apple")

            apple.color = "Pink"

            # Already been read this transaction, don't read it again!
            txn.refresh_if_unread(apple)

            self.assertEqual(apple.color, "Pink")
