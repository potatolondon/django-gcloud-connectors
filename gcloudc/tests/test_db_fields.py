import pickle
from datetime import timedelta

from django import forms
from django.core.exceptions import ValidationError
from django.db import (
    connection,
    models,
)
from django.test import override_settings
from gcloudc.db.models.fields.charfields import (
    CharField,
    CharOrNoneField,
)
from gcloudc.db.models.fields.computed import (
    ComputedBooleanField,
    ComputedCharField,
    ComputedIntegerField,
    ComputedPositiveIntegerField,
    ComputedTextField,
)

from gcloudc.db.models.fields.iterable import SetField, ListField
from gcloudc.db.models.fields.related import RelatedSetField, RelatedListField, GenericRelationField
from gcloudc.db.models.fields.json import JSONField

from . import TestCase
from .models import (
    BasicTestModel,
    BinaryFieldModel,
    ModelWithCharField,
    NonIndexedModel,
    PFAwards,
    PFAuthor,
    PFPost,
    ISOther,
    ISStringReferenceModel,
)


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


class CharFieldModelTests(TestCase):
    def test_char_field_with_max_length_set(self):
        test_bytestrings = [(u"01234567891", 11), (u"ążźsęćńół", 17)]

        for test_text, byte_len in test_bytestrings:
            test_instance = ModelWithCharField(char_field_with_max=test_text)
            self.assertRaisesMessage(
                ValidationError,
                "Ensure this value has at most 10 bytes (it has %d)." % byte_len,
                test_instance.full_clean,
            )

    def test_char_field_with_not_max_length_set(self):
        longest_valid_value = u"0123456789" * 150
        too_long_value = longest_valid_value + u"more"

        test_instance = ModelWithCharField(char_field_without_max=longest_valid_value)
        test_instance.full_clean()  # max not reached so it's all good

        test_instance.char_field_without_max = too_long_value
        self.assertRaisesMessage(
            ValidationError, u"Ensure this value has at most 1500 bytes (it has 1504).", test_instance.full_clean
        )

    def test_additional_validators_work(self):
        test_instance = ModelWithCharField(char_field_as_email="bananas")
        self.assertRaisesMessage(ValidationError, "failed", test_instance.full_clean)

    def test_too_long_max_value_set(self):
        try:

            class TestModel(models.Model):
                test_char_field = CharField(max_length=1501)

        except AssertionError as e:
            self.assertEqual(str(e), "CharFields max_length must not be greater than 1500 bytes.")


class ModelWithCharOrNoneField(models.Model):
    char_or_none_field = CharOrNoneField(max_length=100)


class CharOrNoneFieldTests(TestCase):
    def test_char_or_none_field(self):
        # Ensure that empty strings are coerced to None on save
        obj = ModelWithCharOrNoneField.objects.create(char_or_none_field="")
        obj.refresh_from_db()
        self.assertIsNone(obj.char_or_none_field)


class StringReferenceRelatedSetFieldModelTests(TestCase):
    def test_can_update_related_field_from_form(self):
        related = ISOther.objects.create()
        thing = ISStringReferenceModel.objects.create(related_things={related})
        before_set = thing.related_things
        thing.related_list.field.save_form_data(thing, set())
        thing.save()
        self.assertNotEqual(before_set.all(), thing.related_things.all())

    def test_saving_forms(self):
        class TestForm(forms.ModelForm):
            class Meta:
                model = ISStringReferenceModel
                fields = ("related_things",)

        related = ISOther.objects.create()
        post_data = {"related_things": [str(related.pk)]}

        form = TestForm(post_data)
        self.assertTrue(form.is_valid())
        instance = form.save()
        self.assertEqual({related.pk}, instance.related_things_ids)


class RelatedFieldPrefetchTests(TestCase):
    def test_prefetch_related(self):
        award = PFAwards.objects.create(name="award")
        author = PFAuthor.objects.create(awards={award})
        PFPost.objects.create(authors={author})

        posts = list(PFPost.objects.all().prefetch_related("authors__awards"))

        with self.assertNumQueries(0):
            list(posts[0].authors.all()[0].awards.all())


class PickleTests(TestCase):
    def test_all_fields_are_pickleable(self):
        """ In order to work with Djangae's migrations, all fields must be pickeable. """
        fields = [
            CharField(),
            CharOrNoneField(),
            ComputedBooleanField("method_name"),
            ComputedCharField("method_name"),
            ComputedIntegerField("method_name"),
            ComputedPositiveIntegerField("method_name"),
            ComputedTextField("method_name"),
            GenericRelationField(),
            JSONField(default=list),
            ListField(CharField(), default=["badger"]),
            SetField(CharField(), default=set(["badger"])),
        ]

        fields.extend(
            [RelatedListField(ModelWithCharField), RelatedSetField(ModelWithCharField)]
        )

        for field in fields:
            try:
                pickle.dumps(field)
            except (pickle.PicklingError, TypeError) as e:
                self.fail("Could not pickle %r: %s" % (field, e))


class BinaryFieldModelTests(TestCase):
    binary_value = b"\xff"

    def test_insert(self):

        obj = BinaryFieldModel.objects.create(binary=self.binary_value)
        obj.save()

        readout = BinaryFieldModel.objects.get(pk=obj.pk)

        assert readout.binary == self.binary_value

    def test_none(self):

        obj = BinaryFieldModel.objects.create()
        obj.save()

        readout = BinaryFieldModel.objects.get(pk=obj.pk)

        assert readout.binary is None

    def test_update(self):

        obj = BinaryFieldModel.objects.create()
        obj.save()

        toupdate = BinaryFieldModel.objects.get(pk=obj.pk)
        toupdate.binary = self.binary_value
        toupdate.save()

        readout = BinaryFieldModel.objects.get(pk=obj.pk)

        assert readout.binary == self.binary_value


class CharFieldModel(models.Model):
    char_field = models.CharField(max_length=500)


class CharFieldModelTest(TestCase):
    def test_query(self):
        instance = CharFieldModel(char_field="foo")
        instance.save()

        readout = CharFieldModel.objects.get(char_field="foo")
        self.assertEqual(readout, instance)

    def test_query_unicode(self):
        name = u"Jacqu\xe9s"

        instance = CharFieldModel(char_field=name)
        instance.save()

        readout = CharFieldModel.objects.get(char_field=name)
        self.assertEqual(readout, instance)

    @override_settings(DEBUG=True)
    def test_query_unicode_debug(self):
        """ Test that unicode query can be performed in DEBUG mode,
            which will use CursorDebugWrapper and call last_executed_query.
        """
        name = u"Jacqu\xe9s"

        instance = CharFieldModel(char_field=name)
        instance.save()

        readout = CharFieldModel.objects.get(char_field=name)
        self.assertEqual(readout, instance)


class DurationFieldModelWithDefault(models.Model):
    duration = models.DurationField(default=timedelta(1, 0))


class DurationFieldModelTests(TestCase):
    def test_creates_with_default(self):
        instance = DurationFieldModelWithDefault()

        self.assertEqual(instance.duration, timedelta(1, 0))

        instance.save()

        readout = DurationFieldModelWithDefault.objects.get(pk=instance.pk)
        self.assertEqual(readout.duration, timedelta(1, 0))

    def test_none_saves_as_default(self):
        instance = DurationFieldModelWithDefault()
        # this could happen if we were reading an existing instance out of the database that didn't have this field
        instance.duration = None
        instance.save()

        readout = DurationFieldModelWithDefault.objects.get(pk=instance.pk)
        self.assertEqual(readout.duration, timedelta(1, 0))


class ModelWithNonNullableFieldAndDefaultValue(models.Model):
    some_field = models.IntegerField(null=False, default=1086)


class NonIndexedModelFieldsTests(TestCase):
    def test_long_textfield(self):
        long_text = "A" * 1501
        instance = NonIndexedModel()
        instance.content = long_text
        instance.save()

    def test_big_binaryfield(self):
        long_binary = ("A" * 1501).encode('utf-8')
        instance = NonIndexedModel()
        instance.binary = long_binary
        instance.save()


# ModelWithNonNullableFieldAndDefaultValueTests verifies that we maintain same
# behavior as Django with respect to a model field that is non-nullable with default value.
class ModelWithNonNullableFieldAndDefaultValueTests(TestCase):
    def _create_instance_with_null_field_value(self):

        instance = ModelWithNonNullableFieldAndDefaultValue.objects.create(some_field=1)
        client = connection.connection.gclient
        entity = client.get(
            client.key(
                ModelWithNonNullableFieldAndDefaultValue._meta.db_table,
                instance.pk,
                namespace=connection.settings_dict.get("NAMESPACE", ""),
            )
        )
        del entity["some_field"]
        client.put(entity)

        instance.refresh_from_db()

        return instance

    def test_none_in_db_reads_as_none_in_model(self):
        instance = self._create_instance_with_null_field_value()
        self.assertIsNone(instance.some_field)

    def test_none_in_model_saved_as_default(self):

        instance = self._create_instance_with_null_field_value()

        instance.save()
        instance.refresh_from_db()

        self.assertEqual(instance.some_field, 1086)
