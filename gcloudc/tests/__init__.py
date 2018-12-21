from django.test import TestCase
from django.db import models


class BasicTestModel(models.Model):
    field1 = models.CharField(max_length=100)


class BasicTest(TestCase):
    def test_basic_connector_usage(self):
        # Create
        instance = BasicTestModel.objects.create(field1="Hello World!")

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
