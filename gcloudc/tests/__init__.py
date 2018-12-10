from django.test import TestCase
from django.db import models


class BasicTestModel(models.Model):
    pass


class BasicTest(TestCase):
    def test_basic_connector_usage(self):
        instance = BasicTestModel.objects.create()

        self.assertEqual(1, BasicTestModel.objects.count())
        self.assertEqual(instance, BasicTestModel.objects.get())
