from django.db import models
from django.core.validators import EmailValidator
from gcloudc.db.models.fields.charfields import (
    CharField
)
from gcloudc.db.models.fields.computed import ComputedCollationField


class BinaryFieldModel(models.Model):
    binary = models.BinaryField(null=True)


class ModelWithCharField(models.Model):
    char_field_with_max = CharField(max_length=10, default="", blank=True)
    char_field_without_max = CharField(default="", blank=True)
    char_field_as_email = CharField(max_length=100, validators=[EmailValidator(message="failed")], blank=True)


class TestUser(models.Model):
    """Basic model defintion for use in test cases."""

    username = models.CharField(max_length=32, unique=True)
    first_name = models.CharField(max_length=50)
    second_name = models.CharField(max_length=50)

    def __unicode__(self):
        return self.username

    class Meta:
        unique_together = ("first_name", "second_name")


class TestUserTwo(models.Model):
    username = models.CharField(max_length=32, unique=True)

    class Djangae:
        enforce_constraint_checks = True


class BasicTestModel(models.Model):
    field1 = models.CharField(max_length=100)
    field2 = models.IntegerField(unique=True)


class MultiQueryModel(models.Model):
    field1 = models.IntegerField(null=True)
    field2 = models.CharField(max_length=64)


class ModelWithComputedCollationField(models.Model):
    """Test model for `ComputedCollationField`."""

    name = models.CharField(max_length=100)
    name_order = ComputedCollationField("name")

    class Meta:  # noqa
        app_label = "gcloudc"
