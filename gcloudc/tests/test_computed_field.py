from . import TestCase
from django.db import models
from gcloudc.db.models.fields.computed import ComputedCharField


class ComputedFieldModel(models.Model):
    def computer(self):
        return "%s_%s" % (self.int_field, self.char_field)

    int_field = models.IntegerField()
    char_field = models.CharField(max_length=50)
    test_field = ComputedCharField(computer, max_length=50)
    method_calc_field = ComputedCharField("computer", max_length=50)

    class Meta:
        app_label = "gcloudc"


# ------------------------


class ComputedFieldTests(TestCase):
    def test_computed_field(self):
        instance = ComputedFieldModel(int_field=1, char_field="test")
        instance.save()
        self.assertEqual(instance.test_field, "1_test")

        # Try getting and saving the instance again
        instance = ComputedFieldModel.objects.get(test_field="1_test")
        instance.save()

    def test_computed_by_method_name_field(self):
        """ Test that a computed field which specifies its "computer" function as a string of
            the name of a method on the model.
        """
        instance = ComputedFieldModel(int_field=2, char_field="test")
        instance.save()
        self.assertEqual(instance.method_calc_field, "2_test")
