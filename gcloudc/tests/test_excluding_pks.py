from django.db import models

from . import TestCase


class ExcludedPKModel(models.Model):
    name = models.CharField(max_length=30, primary_key=True)
    color = models.CharField(max_length=30)


class ExcludingPKTests(TestCase):
    def test_exclude_pks_with_slice(self):
        for i in range(10):
            ExcludedPKModel.objects.create(name=str(i), color=str(i))

        to_exclude = [str(x) for x in list(range(5)) + list(range(15, 20))]

        to_return = ExcludedPKModel.objects.exclude(pk__in=set(to_exclude)).values_list("pk", flat=True)[:2]
        self.assertEqual(2, len(to_return))

        qs = ExcludedPKModel.objects.filter(pk__in=to_return)

        self.assertEqual(2, len(qs))

    def test_count_on_excluded_pks(self):
        ExcludedPKModel.objects.create(name="Apple", color="Red")
        ExcludedPKModel.objects.create(name="Orange", color="Orange")

        self.assertEqual(
            1, ExcludedPKModel.objects.filter(pk__in=["Apple", "Orange"]).exclude(pk__in=["Apple"]).count()
        )
