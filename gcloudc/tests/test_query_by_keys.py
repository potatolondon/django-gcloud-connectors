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
