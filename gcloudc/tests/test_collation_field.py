from . import TestCase

from django.db import models
from gcloudc.db.models.fields.computed import ComputedCollationField


class ModelWithComputedCollationField(models.Model):
    """Test model for `ComputedCollationField`."""

    name = models.CharField(max_length=100)
    name_order = ComputedCollationField('name')

    class Meta:  # noqa
        app_label = "gcloudc"


class ComputedCollationFieldTests(TestCase):
    """Tests for `ComputedCollationField`."""

    def test_model(self):
        """Tests for a model using a `ComputedCollationField`."""
        ModelWithComputedCollationField.objects.create(name='demo1')

