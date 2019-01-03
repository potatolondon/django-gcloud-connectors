from django.db import (
    NotSupportedError,
    models,
)
from django.test import override_settings

import sleuth

from . import TestCase


class MultiQueryModel(models.Model):
    field1 = models.IntegerField(null=True)
    field2 = models.CharField(max_length=64)


class AsyncMultiQueryTest(TestCase):
    """
        Specific tests for multiquery
    """

    def test_hundred_or(self):
        for i in range(100):
            MultiQueryModel.objects.create(field1=i)

        self.assertEqual(
            len(MultiQueryModel.objects.filter(field1__in=list(range(100)))),
            100
        )

        self.assertEqual(
            MultiQueryModel.objects.filter(field1__in=list(range(100))).count(),
            100
        )

        self.assertItemsEqual(
            MultiQueryModel.objects.filter(
                field1__in=list(range(100))
            ).values_list("field1", flat=True),
            list(range(100))
        )

        self.assertItemsEqual(
            MultiQueryModel.objects.filter(
                field1__in=list(range(100))
            ).order_by("-field1").values_list("field1", flat=True),
            list(range(100))[::-1]
        )

    @override_settings(DJANGAE_MAX_QUERY_BRANCHES=10)
    def test_max_limit_enforced(self):
        for i in range(11):
            MultiQueryModel.objects.create(field1=i)

        self.assertRaises(
            NotSupportedError,
            list, MultiQueryModel.objects.filter(field1__in=list(range(11)))
        )

    def test_pk_in_with_slicing(self):
        i1 = MultiQueryModel.objects.create()

        self.assertFalse(
            MultiQueryModel.objects.filter(pk__in=[i1.pk])[9999:]
        )

        self.assertFalse(
            MultiQueryModel.objects.filter(pk__in=[i1.pk])[9999:10000]
        )

    def test_limit_correctly_applied_per_branch(self):
        MultiQueryModel.objects.create(field2="test")
        MultiQueryModel.objects.create(field2="test2")

        with sleuth.watch('google.cloud.datastore.query.Query.fetch') as run_calls:

            list(MultiQueryModel.objects.filter(field2__in=["test", "test2"])[:1])

            self.assertEqual(1, run_calls.calls[0].kwargs['limit'])
            self.assertEqual(1, run_calls.calls[1].kwargs['limit'])

        with sleuth.watch('google.cloud.datastore.query.Query.fetch') as run_calls:

            list(MultiQueryModel.objects.filter(field2__in=["test", "test2"])[1:2])

            self.assertEqual(2, run_calls.calls[0].kwargs['limit'])
            self.assertEqual(2, run_calls.calls[1].kwargs['limit'])
