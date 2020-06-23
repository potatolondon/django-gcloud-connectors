import uuid
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

    last_login = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.username

    class Meta:
        unique_together = ("first_name", "second_name")


class TestUserTwo(models.Model):
    username = models.CharField(max_length=32, unique=True)


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

    def __str__(self):
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


class IntegerModel(models.Model):
    integer_field = models.IntegerField()


class NullDate(models.Model):
    date = models.DateField(null=True, default=None)
    datetime = models.DateTimeField(null=True, default=None)
    time = models.TimeField(null=True, default=None)


class NullDateSet(models.Model):
    dates = RelatedSetField(NullDate, blank=True, unique=True)


class UniqueModel(models.Model):
    unique_field = models.CharField(max_length=100, unique=True)
    unique_combo_one = models.IntegerField(blank=True, default=0)
    unique_combo_two = models.CharField(max_length=100, blank=True, default="")

    unique_relation = models.ForeignKey('self', null=True, blank=True, unique=True, on_delete=models.DO_NOTHING)

    unique_set_field = SetField(models.CharField(max_length=500), unique=True)
    unique_list_field = ListField(models.CharField(max_length=500), unique=True)

    unique_together_list_field = ListField(models.IntegerField())

    class Meta:
        unique_together = [
            ("unique_combo_one", "unique_combo_two"),
            ("unique_together_list_field", "unique_combo_one")
        ]


class ModelWithNullableCharField(models.Model):
    field1 = models.CharField(max_length=500, null=True)
    some_id = models.IntegerField(default=0)


class DurationModel(models.Model):
    duration_field = models.DurationField()
    duration_field_nullable = models.DurationField(blank=True, null=True)


class ModelWithUniques(models.Model):
    name = models.CharField(max_length=64, unique=True)


class ModelWithUniquesOnForeignKey(models.Model):
    name = models.CharField(max_length=64, unique=True)
    related_name = models.ForeignKey(ModelWithUniques, unique=True, on_delete=models.DO_NOTHING)

    class Meta:
        unique_together = [("name", "related_name")]


class UniqueModelWithLongPK(models.Model):
    long_pk = models.CharField(max_length=500, primary_key=True)
    unique_field = models.IntegerField(unique=True)


class Zoo(models.Model):
    pass


class Enclosure(models.Model):
    zoo = models.ForeignKey(Zoo, on_delete=models.CASCADE)


class Animal(models.Model):
    enclosure = models.ForeignKey(Enclosure, on_delete=models.CASCADE)


class UUIDTestModel(models.Model):
    uuid = models.UUIDField(default=uuid.uuid4)


class SpecialIndexesModel(models.Model):
    name = models.CharField(max_length=255, primary_key=True)
    nickname = CharField(blank=True)
    sample_list = ListField(models.CharField)

    def __str__(self):
        return self.name


class Permission(models.Model):
    user = models.ForeignKey(TestUser, on_delete=models.CASCADE)
    perm = models.CharField(max_length=32)

    def __str__(self):
        return u"{0} for {1}".format(self.perm, self.user)

    class Meta:
        ordering = ('user__username', 'perm')


class SelfRelatedModel(models.Model):
    related = models.ForeignKey('self', blank=True, null=True, on_delete=models.SET_NULL)


class MultiTableParent(models.Model):
    parent_field = models.CharField(max_length=32)


class ModelWithDates(models.Model):
    start = models.DateField()
    end = models.DateField()


class MultiTableChildOne(MultiTableParent):
    child_one_field = models.CharField(max_length=32)


class MultiTableChildTwo(MultiTableParent):
    child_two_field = models.CharField(max_length=32)


class DateTimeModel(models.Model):
    datetime_field = models.DateTimeField(auto_now_add=True)
    date_field = models.DateField(auto_now_add=True)


class NonIndexedModel(models.Model):
    content = models.TextField()
    binary = models.BinaryField()


class NullableFieldModel(models.Model):
    nullable = models.IntegerField(null=True)


class Thing(models.Model):
    num = models.IntegerField(default=0)
