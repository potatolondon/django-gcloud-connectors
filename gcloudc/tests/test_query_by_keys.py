import google

from . import TestCase
from .models import NullableFieldModel


class QueryByKeysTest(TestCase):
    """
        Tests for the Get optimisation when keys are
        included in all branches of a query.
    """

    databases = "__all__"

    def test_missing_results_are_skipped(self):
        NullableFieldModel.objects.create(pk=1)
        NullableFieldModel.objects.create(pk=5)

        results = NullableFieldModel.objects.filter(
            pk__in=[1, 2, 3, 4, 5]
        ).order_by("nullable").values_list("pk", flat=True)

        self.assertCountEqual(results, [1, 5])

    def test_none_namespace(self):
        NullableFieldModel.objects.using("nonamespace").create(pk=1)
        NullableFieldModel.objects.using("nonamespace").create(pk=5)

        results = NullableFieldModel.objects.using(
            "nonamespace").filter(
                pk__in=[1, 2, 3, 4, 5]
        ).order_by("nullable").values_list("pk", flat=True)

        self.assertCountEqual(results, [1, 5])

    def test_large_number_of_keys(self):
        keys = []

        for i in range(1001):
            keys.append(NullableFieldModel.objects.create(pk=i + 1).pk)

        try:
            results = list(NullableFieldModel.objects.filter(pk__in=keys))
        except google.api_core.exceptions.InvalidArgument:
            self.fail("Didn't correctly deal with a large number of keys")

        self.assertEqual(len(results), 1001)
        self.assertCountEqual([x.pk for x in results], keys)
