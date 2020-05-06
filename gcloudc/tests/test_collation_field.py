from . import TestCase
from .models import ModelWithComputedCollationField


class ComputedCollationFieldTests(TestCase):
    """Tests for `ComputedCollationField`."""

    def test_model(self):
        """Tests for a model using a `ComputedCollationField`."""
        ModelWithComputedCollationField.objects.create(name="demo1")
