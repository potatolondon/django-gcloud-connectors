from django.db import models
from django.test import TestCase as DjangoTestCase

from gcloudc.db.models.fields.iterable import (
    ListField,
    SetField
)

from gcloudc.db.models.fields.related import (
    RelatedListField,
    RelatedSetField,
)


class TestCase(DjangoTestCase):
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


class ISOther(models.Model):
    name = models.CharField(max_length=500)

    def __str__(self):
        return "%s:%s" % (self.pk, self.name)

    class Meta:
        app_label = "gcloudc"


class IterableFieldsWithValidatorsModel(models.Model):
    set_field = SetField(
        models.CharField(max_length=100),
        min_length=2, max_length=3, blank=False
    )

    list_field = ListField(
        models.CharField(max_length=100),
        min_length=2, max_length=3, blank=False
    )

    related_set = RelatedSetField(
        ISOther, min_length=2, max_length=3, blank=False
    )

    related_list = RelatedListField(
        ISOther,
        related_name="iterable_list",
        min_length=2, max_length=3, blank=False
    )

    class Meta:
        app_label = "gcloudc"


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
