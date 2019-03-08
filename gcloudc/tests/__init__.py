import os

from django.test import TestCase as DjangoTestCase
from django.db import models

from google.cloud import datastore, environment_vars

import requests


def _init_datastore_client():
    return datastore.Client(
        namespace=None,
        project="test",
        _http=requests.Session if os.environ.get(environment_vars.GCD_HOST) else None,
    )

def get_kind_query(kind, keys_only=True):
    datastore_client = _init_datastore_client()
    query = datastore_client.query(kind=kind)
    if keys_only:
        query.keys_only()
    return list(query.fetch())


class TestCase(DjangoTestCase):

    # FIXME this is a temporary/nuclear option to wipe the datastore of all entities
    # between each unit test - eventually we hope we can use the transaction
    # logic in vanilla django or better embed this into a test runner setup etc
    KINDS_TO_DELETE = []

    def setUp(self):
        super(TestCase, self).setUp()
        self.datastore_client = _init_datastore_client()

        for kind in self.KINDS_TO_DELETE:
            query = self.datastore_client.query(kind=kind)
            query.keys_only()
            results = list(query.fetch())
            if results:
                self.datastore_client.delete_multi([result.key for result in results])

    def tearDown(self):
        super(TestCase, self).tearDown()
        for kind in self.KINDS_TO_DELETE:
            query = self.datastore_client.query(kind=kind)
            query.keys_only()
            results = list(query.fetch())
            if results:
                self.datastore_client.delete_multi([result.key for result in results])

    # This was mistakenly renamed to assertCountsEqual
    # in Python 3, so this avoids any complications arising
    # when they rectify that! https://bugs.python.org/issue27060
    def assertItemsEqual(self, lhs, rhs):
        if set(lhs) != set(rhs):
            raise AssertionError(
                "Items were not the same in both lists"
            )


class BasicTestModel(models.Model):
    field1 = models.CharField(max_length=100)
    field2 = models.IntegerField(unique=True)


class BasicTest(TestCase):
    def test_basic_connector_usage(self):
        # Create
        instance = BasicTestModel.objects.create(field1="Hello World!", field2=1998)

        # Count
        self.assertEqual(1, BasicTestModel.objects.count())

        # Get
        self.assertEqual(instance, BasicTestModel.objects.get())

        # Update
        instance.field1 = "Hello Mars!"
        instance.save()

        # Query
        instance2 = BasicTestModel.objects.filter(field1="Hello Mars!")[0]

        self.assertEqual(instance, instance2)
        self.assertEqual(instance.field1, instance2.field1)

        # Query by PK
        instance2 = BasicTestModel.objects.filter(pk=instance.pk)[0]

        self.assertEqual(instance, instance2)
        self.assertEqual(instance.field1, instance2.field1)

        # Non-existent PK
        instance3 = BasicTestModel.objects.filter(pk=999).first()
        self.assertIsNone(instance3)

        # Unique field
        instance2 = BasicTestModel.objects.filter(field2=1998)[0]

        self.assertEqual(instance, instance2)
        self.assertEqual(instance.field1, instance2.field1)
