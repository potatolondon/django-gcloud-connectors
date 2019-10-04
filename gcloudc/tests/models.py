from django.db import models
from django.core.validators import EmailValidator
from gcloudc.db.models.fields.charfields import (
    CharField
)
from gcloudc.db.models.fields.computed import ComputedCollationField
from gcloudc.db.models.fields.related import RelatedSetField, RelatedListField, GenericRelationField
from gcloudc.db.models.fields.iterable import SetField, ListField


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
    email = models.EmailField(blank=True, default="")
    field2 = models.CharField(max_length=32, blank=True, default="")

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


class PFPost(models.Model):
    content = models.TextField()
    authors = RelatedSetField('PFAuthor', related_name='posts')


class PFAuthor(models.Model):
    name = models.CharField(max_length=32)
    awards = RelatedSetField('PFAwards')


class PFAwards(models.Model):
    name = models.CharField(max_length=32)


class ISOther(models.Model):
    name = models.CharField(max_length=500)

    def __str__(self):
        return "%s:%s" % (self.pk, self.name)


class RelationWithoutReverse(models.Model):
    name = models.CharField(max_length=500)


class ISModel(models.Model):
    related_things = RelatedSetField(ISOther)
    related_list = RelatedListField(ISOther, related_name="ismodel_list")
    limted_related = RelatedSetField(RelationWithoutReverse, limit_choices_to={"name": "banana"}, related_name="+")
    children = RelatedSetField("self", related_name="+")


class GenericRelationModel(models.Model):
    relation_to_anything = GenericRelationField(null=True)
    unique_relation_to_anything = GenericRelationField(null=True, unique=True)


class IterableFieldsWithValidatorsModel(models.Model):
    set_field = SetField(models.CharField(max_length=100), min_length=2, max_length=3, blank=False)
    list_field = ListField(models.CharField(max_length=100), min_length=2, max_length=3, blank=False)
    related_set = RelatedSetField(ISOther, min_length=2, max_length=3, blank=False)
    related_list = RelatedListField(ISOther, related_name="iterable_list", min_length=2, max_length=3, blank=False)


class ModelDatabaseA(models.Model):
    set_of_bs = RelatedSetField("ModelDatabaseB", related_name="+")
    list_of_bs = RelatedListField("ModelDatabaseB", related_name="+")


class ModelDatabaseB(models.Model):
    test_database = "ns1"


class IterableRelatedModel(models.Model):
    related_set = RelatedListField(ISOther, related_name="+")
    related_list = RelatedListField(ISOther, related_name="+")


class RelationWithOverriddenDbTable(models.Model):
    class Meta:
        db_table = "bananarama"


class Post(models.Model):
    content = models.TextField()
    tags = RelatedSetField("Tag", related_name="posts")
    ordered_tags = RelatedListField("Tag")


class Tag(models.Model):
    name = models.CharField(max_length=64)


class RelatedCharFieldModel(models.Model):
    char_field = CharField(max_length=500)


class StringPkModel(models.Model):
    name = models.CharField(max_length=500, primary_key=True)


class IterableRelatedWithNonIntPkModel(models.Model):
    related_set = RelatedListField(StringPkModel, related_name="+")
    related_list = RelatedListField(StringPkModel, related_name="+")


class RelatedListFieldRemoveDuplicatesModel(models.Model):
    related_list_field = RelatedListField(RelatedCharFieldModel, remove_duplicates=True)


class ISStringReferenceModel(models.Model):
    related_things = RelatedSetField('ISOther')
    related_list = RelatedListField('ISOther', related_name="ismodel_list_string")
    limted_related = RelatedSetField('RelationWithoutReverse', limit_choices_to={'name': 'banana'}, related_name="+")
    children = RelatedSetField("self", related_name="+")


class TestFruit(models.Model):
    name = models.CharField(primary_key=True, max_length=32)
    origin = models.CharField(max_length=32, default="Unknown")
    color = models.CharField(max_length=100)
    is_mouldy = models.BooleanField(default=False)
    text_field = models.TextField(blank=True, default="")
    binary_field = models.BinaryField(blank=True)

    class Meta:
        ordering = ("color",)

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return "<TestFruit: name={}, color={}>".format(self.name, self.color)


class TransformTestModel(models.Model):
    field1 = models.CharField(max_length=255)
    field2 = models.CharField(max_length=255, unique=True)
    field3 = models.CharField(null=True, max_length=255)
    field4 = models.TextField()


class InheritedModel(TransformTestModel):
    pass


class Relation(models.Model):
    pass


class Related(models.Model):
    headline = models.CharField(max_length=500)
    relation = models.ForeignKey(Relation, on_delete=models.DO_NOTHING)
