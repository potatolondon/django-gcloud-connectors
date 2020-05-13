from . import TestCase
from .models import NullableFieldModel


class QueryByKeysTest(TestCase):
    """
        Tests for the Get optimisation when keys are
        included in all branches of a query.
    """

    def test_missing_results_are_skipped(self):
        NullableFieldModel.objects.create(pk=1)
        NullableFieldModel.objects.create(pk=5)

        results = NullableFieldModel.objects.filter(
            pk__in=[1, 2, 3, 4, 5]
        ).order_by("nullable")

        self.assertCountEqual(results, [1, 5])
