from django.db import (
    NotSupportedError,
    models,
)

from . import TestCase

from django.test import (
    override_settings,
)


class MultiQueryModel(models.Model):
    field1 = models.IntegerField()


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
