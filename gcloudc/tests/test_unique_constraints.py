import sleuth

from django.db import connection
from django.db.utils import IntegrityError
from django.test.utils import override_settings

from gcloudc.db.backends.datastore.unique_utils import _format_value_for_identifier
from gcloudc.db.backends.datastore.transaction import TransactionFailedError
from google.cloud.datastore.key import Key

from . import TestCase

from .models import TestUser, TestUserTwo


def _get_client():
    return connection.connection.gclient


def get_kind_query(kind, keys_only=True):
    datastore_client = _get_client()
    query = datastore_client.query(kind=kind)
    if keys_only:
        query.keys_only()
    return list(query.fetch())


class TestUniqueConstraints(TestCase):

    KINDS_TO_DELETE = ["uniquemarker", "test_testuser", "test_testusertwo"]

    def test_insert(self):
        """
        Assert that when creating a new instance, unique markers are also
        created to reflect the constraints defined on the model.

        If a subsequent insert is attempted, these should be compared to
        enforce a constraint similar to SQL.
        """
        user = TestUser.objects.create(username="tommyd", first_name="Tommy", second_name="Doherty")

        # unique_markers = get_kind_query("uniquemarker", keys_only=True)
        # self.assertEqual(len(unique_markers), 2)

        # attempt to create another entity which violates one of the constraints
        with self.assertRaises(IntegrityError):
            TestUser.objects.create(username="thetommyd", first_name="Tommy", second_name="Doherty")

        # # there should still only be two unique markers, both referencing
        # # the original entity
        # unique_markers = get_kind_query("uniquemarker", keys_only=False)
        # self.assertEqual(len(unique_markers), 2)
        # for marker in unique_markers:
        #     key = Key(
        #         TestUser._meta.db_table, user.pk, project="test",
        #         namespace=connection.settings_dict["NAMESPACE"]
        #     )
        #     self.assertEqual(marker["instance"], key)

    def test_bulk_insert(self):
        """
        Assert that bulk inserts respect any unique markers made inside
        the same transaction.
        """
        with self.assertRaises(IntegrityError):
            TestUserTwo.objects.bulk_create(
                [
                    TestUserTwo(username="Mickey Bell"),
                    TestUserTwo(username="Tony Thorpe"),
                    TestUserTwo(username="Mickey Bell"),
                ]
            )

        self.assertEqual(TestUserTwo.objects.count(), 0)

        # sanity check normal bulk insert works
        TestUserTwo.objects.bulk_create([TestUserTwo(username="Mickey Bell"), TestUserTwo(username="Tony Thorpe")])
        self.assertEqual(TestUserTwo.objects.count(), 2)

        # and if we were to run the bulk insert, previously created
        # unique markers are still respected
        with self.assertRaises(IntegrityError):
            TestUserTwo.objects.bulk_create([TestUserTwo(username="Mickey Bell"), TestUserTwo(username="Tony Thorpe")])
        self.assertEqual(TestUserTwo.objects.count(), 2)

    def test_if_subsequent_marker_put_fails(self):
        """
        Assert that when bulk inserting, if an exception is raised in the marker
        independent transaction (not the first), all put() operations are rolled
        back to mimic a single atomic block.
        """
        user = TestUserTwo.objects.create(username="Tony Thorpe")

        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 1)

        with self.assertRaises(IntegrityError):
            TestUserTwo.objects.bulk_create([TestUserTwo(username="Mickey Bell"), TestUserTwo(username="Tony Thorpe")])

        # neither insert should have worked
        self.assertEqual(TestUserTwo.objects.count(), 1)

        # there should be no new unique markers
        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        self.assertEqual(len(unique_markers), 1)

        # the previous unique markers should have remained
        key = Key(
            TestUserTwo._meta.db_table, user.pk, project="test",
            namespace=connection.settings_dict["NAMESPACE"]
        )
        self.assertEqual(unique_markers[0]["instance"], key)

    @override_settings(ENFORCE_CONSTRAINT_CHECKS=False)
    def test_insert_with_global_unique_checks_disabled(self):
        """
        Assert that the global flag is respected for insertions, such that
        any unique constraints defined on the model are ignored.
        """
        user_kwargs = {"username": "tonyt", "first_name": "Tony", "second_name": "Thorpe"}

        TestUser.objects.create(**user_kwargs)
        TestUser.objects.create(**user_kwargs)

        self.assertEqual(TestUser.objects.count(), 2)

        # when unique constraints are disabled, it also means that we don't
        # create any additional UniqueMarker entities on put()
        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 0)

    @override_settings(ENFORCE_CONSTRAINT_CHECKS=False)
    def test_insert_with_model_settings_precident(self):
        """
        Assert that despite unique constraints being disabled globally,
        on a per model basis it can be enabled.
        """
        # so for the model without the explicit model level setting,
        # unique constraints are not applied due to the global flag
        user_kwargs = {"username": "tonythorpe", "first_name": "Tony", "second_name": "Thorpe"}
        TestUser.objects.create(**user_kwargs)
        TestUser.objects.create(**user_kwargs)

        self.assertEqual(TestUser.objects.count(), 2)

        # but for the model where it is, normal behaviour is demonstrated
        user_kwargs = {"username": "BS3"}
        user_three = TestUserTwo.objects.create(**user_kwargs)
        with self.assertRaises(IntegrityError):
            TestUserTwo.objects.create(**user_kwargs)

        # unique markers would also be written in this case
        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        self.assertEqual(len(unique_markers), 1)
        for marker in unique_markers:
            key = Key(
                TestUserTwo._meta.db_table, user_three.pk, project="test",
                namespace=connection.settings_dict["NAMESPACE"]
            )
            self.assertEqual(marker["instance"], key)

    def test_if_marker_put_fails(self):
        """
        Assert that if any UniqueMarker put operation fails, when run
        as part of a transaction kicked off from another entity insert,
        this outer transaction would also fail.
        """
        with sleuth.detonate(
            "gcloudc.db.backends.datastore.constraints.unique_identifiers_from_entity", TransactionFailedError
        ):
            with self.assertRaises(TransactionFailedError):
                TestUser.objects.create(username="mattyh", first_name="Matt", second_name="Hill")

        # there should be no user object...
        self.assertEqual(TestUser.objects.count(), 0)

        # and no unique markers
        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 0)

    def test_insert_ok_if_unique_marker_has_stale_reference(self):
        """
        Assert that when creating a new instance, if a UniqueMarker already
        exists for the given unique constraint combiniation, the operation
        will still succeed if the entity reference is stale (aka that entity
        has been deleted).
        """
        user = TestUserTwo.objects.create(username="mattyh")

        # delete the entity using the raw API to avoid removing the marker
        client = _get_client()
        key = client.key(
            TestUserTwo._meta.db_table, user.pk,
            namespace=connection.settings_dict["NAMESPACE"]
        )
        client.delete(key)

        # the markers will still be there
        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 1)

        new_user = TestUserTwo.objects.create(username="mattyh")

        # the markers should ref the new entity
        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        key = Key(
            TestUserTwo._meta.db_table, new_user.pk, project="test",
            namespace=connection.settings_dict["NAMESPACE"]
        )
        self.assertEqual(unique_markers[0]["instance"], key)

    def test_update_updates_markers(self):
        """
        Assert that previous and now un-necessary unique markers are deleted
        as part of any updated operation, and new markers are created where
        needed.
        """
        user = TestUserTwo.objects.create(username="AshtonGateEight")

        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        self.assertEqual(len(unique_markers), 1)
        for marker in unique_markers:
            # refs the right entity
            key = Key(
                TestUserTwo._meta.db_table, user.pk, project="test",
                namespace=connection.settings_dict["NAMESPACE"]
            )
            self.assertEqual(marker["instance"], key)
            # the named key should ref the unique username value
            self.assertIn(_format_value_for_identifier(user.username), marker.key.name)

        # now do the update operation
        user.username = "Ashton Robin"
        user.save()

        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        self.assertEqual(len(unique_markers), 1)
        for marker in unique_markers:
            # refs the right entity
            key = Key(
                TestUserTwo._meta.db_table, user.pk, project="test",
                namespace=connection.settings_dict["NAMESPACE"]
            )
            self.assertEqual(marker["instance"], key)
            # the named key should ref the new unique value
            self.assertIn(_format_value_for_identifier(user.username), marker.key.name)

    def test_update_with_constraint_conflict(self):
        TestUserTwo.objects.create(username="AshtonGateEight")
        user_two = TestUserTwo.objects.create(username="AshtonGateSeven")

        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        named_keys = [marker.key.name for marker in unique_markers]
        self.assertEqual(len(unique_markers), 2)

        # now do the update operation
        user_two.username = "AshtonGateEight"
        with self.assertRaises(IntegrityError):
            user_two.save()

        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        self.assertEqual(len(unique_markers), 2)
        self.assertItemsEqual(named_keys, [marker.key.name for marker in unique_markers])

    def test_error_on_update_does_not_change_markers(self):
        """
        Assert that when there is an error / exception raised as part of the
        update command, any markers which have been deleted or added are
        rolled back, such that the previous state before the operation
        is maintained.
        """
        user = TestUserTwo.objects.create(username="AshtonGateEight")

        with sleuth.detonate("gcloudc.db.backends.datastore.transaction.Transaction.put", TransactionFailedError):
            with self.assertRaises(TransactionFailedError):
                user.username = "Red Army"
                user.save()

        user.refresh_from_db()
        self.assertEqual(user.username, "AshtonGateEight")

        # the unique markers should still ref the original unique value
        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        self.assertEqual(len(unique_markers), 1)
        self.assertIn(_format_value_for_identifier("AshtonGateEight"), unique_markers[0].key.name)

    def test_bulk_update(self):
        """
        Assert that updates via the QuerySet API handle uniques.
        """
        TestUser.objects.create(username="stevep", first_name="steve", second_name="phillips")
        TestUser.objects.create(username="joeb", first_name="joe", second_name="burnell")
        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 4)

        # now do the update operation on the queryset
        TestUser.objects.all().update(first_name="lee")

        unique_markers = get_kind_query("uniquemarker", keys_only=False)
        self.assertEqual(len(unique_markers), 4)
        for marker in unique_markers:
            if "first_name" in marker.key.name:
                # the named key should ref the new unique value
                self.assertIn(_format_value_for_identifier("lee"), marker.key.name)

    def test_error_with_bulk_update(self):
        # TODO
        pass

    def test_delete_clears_markers(self):
        """
        Any markers associated with a given entity should be purged when
        the entity is deleted (via the ORM).
        """
        user = TestUserTwo.objects.create(username="Mickey Bell")

        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 1)

        user.delete()
        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 0)

    def test_bulk_delete_fails_if_limit_exceeded(self):
        """
        Assert that there is currently a practical limitation when deleting multi
        entities, based on a combination of the unique markers per model
        and transaction limit of touching 500 entities.
        """
        TestUserTwo.objects.create(username="Mickey Bell")
        TestUserTwo.objects.create(username="Tony Thorpe")

        with sleuth.switch("gcloudc.db.backends.datastore.transaction.TRANSACTION_ENTITY_LIMIT", 1):
            with self.assertRaises(Exception):
                TestUserTwo.objects.all().delete()

    def test_delete_entity_fails(self):
        """
        Assert that if the entity delete operation fails, any related
        UniqueMarkers which have been deleted are rolled back.
        """
        user = TestUserTwo.objects.create(username="Mickey Bell")

        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 1)

        with sleuth.detonate(
            "gcloudc.db.backends.datastore.commands.remove_entities_from_cache_by_key", TransactionFailedError
        ):
            with self.assertRaises(TransactionFailedError):
                user.delete()

        # the entity in question should not have been deleted, as error in the
        # transactions atomic block should revert all changes
        user.refresh_from_db()

        # there should still be one unique marker left as the operation failed
        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 1)

    def test_delete_marker_fails(self):
        """
        Assert that if there is an exception raised attempting to delete
        a unique marker, that does not impact the core delete operation.
        """
        user = TestUserTwo.objects.create(username="Mickey Bell")

        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 1)

        with sleuth.detonate(
            "gcloudc.db.backends.datastore.commands.delete_unique_markers_for_entity", TransactionFailedError
        ):
            # we catch the transaction error to facilitate this behaviour
            user.delete()

        # the entity in question should be deleted despite the error
        with self.assertRaises(TestUserTwo.DoesNotExist):
            user.refresh_from_db()

        # the marker will still be there but thats ok!
        unique_markers = get_kind_query("uniquemarker", keys_only=True)
        self.assertEqual(len(unique_markers), 1)
