from datetime import timedelta
from collections import OrderedDict
import pickle

from django import forms
from django.core.exceptions import ValidationError, ImproperlyConfigured
from django.core.validators import EmailValidator
from django.db import models
from django.test import override_settings

from google.cloud import datastore

from gcloudc.db.caching import disable_cache
from gcloudc.db.models.fields.charfields import (
    CharField,
    CharOrNoneField
)
from gcloudc.db.models.fields.computed import (
    ComputedBooleanField,
    ComputedCharField,
    ComputedIntegerField,
    ComputedPositiveIntegerField,
    ComputedTextField
)
from gcloudc.tests import ISOther
from gcloudc.db.models.fields.related import (
    GenericRelationField,
    RelatedListField,
    RelatedSetField
)
from gcloudc.db.models.fields.json import (
    JSONField
)
from gcloudc.db.models.fields.iterable import (
    ListField,
    SetField
)
from gcloudc.db.models.fields.counting import ShardedCounterField

from . import TestCase


class ISStringReferenceModel(models.Model):
    related_things = RelatedSetField('ISOther')
    related_list = RelatedListField('ISOther', related_name="ismodel_list_string")
    limted_related = RelatedSetField('RelationWithoutReverse', limit_choices_to={'name': 'banana'}, related_name="+")
    children = RelatedSetField("self", related_name="+")


class PFPost(models.Model):
    content = models.TextField()
    authors = RelatedSetField('PFAuthor', related_name='posts')


class PFAuthor(models.Model):
    name = models.CharField(max_length=32)
    awards = RelatedSetField('PFAwards')


class PFAwards(models.Model):
    name = models.CharField(max_length=32)


class ModelWithNonNullableFieldAndDefaultValue(models.Model):
    some_field = models.IntegerField(null=False, default=1086)


class JSONFieldModel(models.Model):
    json_field = JSONField(use_ordered_dict=True, blank=True)


class JSONFieldWithDefaultModel(models.Model):
    json_field = JSONField(use_ordered_dict=True)


class JSONFieldModelTests(TestCase):

    def test_invalid_data_in_datastore_doesnt_throw_an_error(self):
        """
            If invalid data is found while reading the entity data, then
            we should silently ignore the error and just return the data as-is
            rather than converting to list/dict.
            The reason is that if we blow up on load, then there's no way to load the
            entity (in Django) to repair the data. This is also consistent with the behaviour
            of Django when (for example) you load a NULL from the database into a field that is
            non-nullable. The field value will still be None when read.
        """
        from django.conf import settings
        from django.db import connections
        from google.cloud.datastore.entity import Entity
        from google.cloud.datastore.key import Key

        conn = connections['default'].get_new_connection(
            connections['default'].settings_dict
        )

        client = conn.gclient
        key = client.key(JSONFieldModel._meta.db_table, 1)
        entity = Entity(key=key)
        entity["json_field"] = "bananas"
        client.put(entity)

        instance = JSONFieldModel.objects.get(pk=1)
        self.assertEqual(instance.json_field, "bananas")

    def test_object_pairs_hook_with_ordereddict(self):
        items = [('first', 1), ('second', 2), ('third', 3), ('fourth', 4)]
        od = OrderedDict(items)

        thing = JSONFieldModel(json_field=od)
        thing.save()

        thing = JSONFieldModel.objects.get()
        self.assertEqual(od, thing.json_field)

    def test_object_pairs_hook_with_normal_dict(self):
        """
        Check that dict is not stored as OrderedDict if
        object_pairs_hook is not set
        """

        # monkey patch field
        field = JSONFieldModel._meta.get_field('json_field')
        field.use_ordered_dict = False

        normal_dict = {'a': 1, 'b': 2, 'c': 3}

        thing = JSONFieldModel(json_field=normal_dict)
        self.assertFalse(isinstance(thing.json_field, OrderedDict))
        thing.save()

        thing = JSONFieldModel.objects.get()
        self.assertFalse(isinstance(thing.json_field, OrderedDict))

        field.use_ordered_dict = True

    def test_float_values(self):
        """ Tests that float values in JSONFields are correctly serialized over repeated saves.
            Regression test for 46e685d4, which fixes floats being returned as strings after a second save.
        """
        test_instance = JSONFieldModel(json_field={'test': 0.1})
        test_instance.save()

        test_instance = JSONFieldModel.objects.get()
        test_instance.save()

        test_instance = JSONFieldModel.objects.get()
        self.assertEqual(test_instance.json_field['test'], 0.1)

    def test_defaults_are_handled_as_pythonic_data_structures(self):
        """ Tests that default values are handled as python data structures and
            not as strings. This seems to be a regression after changes were
            made to remove Subfield from the JSONField and simply use TextField
            instead.
        """
        thing = JSONFieldModel()
        self.assertEqual(thing.json_field, {})

    def test_default_value_correctly_handled_as_data_structure(self):
        """ Test that default value - if provided is not transformed into
            string anymore. Previously we needed string, since we used
            SubfieldBase in JSONField. Since it is now deprecated we need
            to change handling of default value.
        """
        thing = JSONFieldWithDefaultModel()
        self.assertEqual(thing.json_field, {})


class ModelWithCharField(models.Model):
    char_field_with_max = CharField(
        max_length=10, default='', blank=True
    )

    char_field_without_max = CharField(
        default='', blank=True
    )

    char_field_as_email = CharField(
        max_length=100, validators=[EmailValidator(message='failed')], blank=True
    )


class ModelWithCharOrNoneField(models.Model):
    char_or_none_field = CharOrNoneField(max_length=100)


class CharFieldModelTests(TestCase):

    def test_char_field_with_max_length_set(self):
        test_bytestrings = [
            (u'01234567891', 11),
            (u'ążźsęćńół', 17),
        ]

        for test_text, byte_len in test_bytestrings:
            test_instance = ModelWithCharField(
                char_field_with_max=test_text,
            )
            self.assertRaisesMessage(
                ValidationError,
                "Ensure this value has at most 10 bytes (it has %d)." % byte_len,
                test_instance.full_clean,
            )

    def test_char_field_with_not_max_length_set(self):
        longest_valid_value = '0123456789' * 150
        too_long_value = longest_valid_value + u'more'

        test_instance = ModelWithCharField(
            char_field_without_max=longest_valid_value,
        )
        test_instance.full_clean()  # max not reached so it's all good

        test_instance.char_field_without_max = too_long_value
        self.assertRaisesMessage(
            ValidationError,
            'Ensure this value has at most 1500 bytes (it has 1504).',
            test_instance.full_clean,
         )

    def test_additional_validators_work(self):
        test_instance = ModelWithCharField(char_field_as_email='bananas')
        self.assertRaisesMessage(ValidationError, 'failed', test_instance.full_clean)

    def test_too_long_max_value_set(self):
        try:
            class TestModel(models.Model):
                test_char_field = models.CharField(max_length=1501)
        except AssertionError as e:
            self.assertEqual(
                e.message,
                'CharFields max_length must not be greater than 1500 bytes.',
            )


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
                fields = ("related_things", )

        related = ISOther.objects.create()
        post_data = {
            "related_things": [ str(related.pk) ],
        }

        form = TestForm(post_data)
        self.assertTrue(form.is_valid())
        instance = form.save()
        self.assertEqual({related.pk}, instance.related_things_ids)


class RelatedFieldPrefetchTests(TestCase):

    def test_prefetch_related(self):
        award = PFAwards.objects.create(name="award")
        author = PFAuthor.objects.create(awards={award})
        post = PFPost.objects.create(authors={author})

        posts = list(PFPost.objects.all().prefetch_related('authors__awards'))

        with self.assertNumQueries(0):
            awards = list(posts[0].authors.all()[0].awards.all())


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

        fields.extend([
            RelatedListField(ModelWithCharField),
            RelatedSetField(ModelWithCharField),
            ShardedCounterField(),
        ])

        for field in fields:
            try:
                pickle.dumps(field)
            except (pickle.PicklingError, TypeError) as e:
                self.fail("Could not pickle %r: %s" % (field, e))


class BinaryFieldModel(models.Model):
    binary = models.BinaryField(null=True)

    class Meta:
        app_label = "djangae"


class BinaryFieldModelTests(TestCase):
    binary_value = b'\xff'

    def test_insert(self):

        obj = BinaryFieldModel.objects.create(binary = self.binary_value)
        obj.save()

        readout = BinaryFieldModel.objects.get(pk = obj.pk)

        assert(readout.binary == self.binary_value)

    def test_none(self):

        obj = BinaryFieldModel.objects.create()
        obj.save()

        readout = BinaryFieldModel.objects.get(pk = obj.pk)

        assert(readout.binary is None)

    def test_update(self):

        obj = BinaryFieldModel.objects.create()
        obj.save()

        toupdate = BinaryFieldModel.objects.get(pk = obj.pk)
        toupdate.binary = self.binary_value
        toupdate.save()

        readout = BinaryFieldModel.objects.get(pk = obj.pk)

        assert(readout.binary == self.binary_value)


class CharFieldModel(models.Model):
    char_field = models.CharField(max_length=500)

    class Meta:
        app_label = "djangae"


class CharFieldModelTest(TestCase):

    def test_query(self):
        instance = CharFieldModel(char_field="foo")
        instance.save()

        readout = CharFieldModel.objects.get(char_field="foo")
        self.assertEqual(readout, instance)

    def test_query_unicode(self):
        name = u'Jacqu\xe9s'

        instance = CharFieldModel(char_field=name)
        instance.save()

        readout = CharFieldModel.objects.get(char_field=name)
        self.assertEqual(readout, instance)

    @override_settings(DEBUG=True)
    def test_query_unicode_debug(self):
        """ Test that unicode query can be performed in DEBUG mode,
            which will use CursorDebugWrapper and call last_executed_query.
        """
        name = u'Jacqu\xe9s'

        instance = CharFieldModel(char_field=name)
        instance.save()

        readout = CharFieldModel.objects.get(char_field=name)
        self.assertEqual(readout, instance)


class DurationFieldModelWithDefault(models.Model):
    duration = models.DurationField(default=timedelta(1,0))

    class Meta:
        app_label = "gcloudc"


class DurationFieldModelTests(TestCase):

    def test_creates_with_default(self):
        instance = DurationFieldModelWithDefault()

        self.assertEqual(instance.duration, timedelta(1,0))

        instance.save()

        readout = DurationFieldModelWithDefault.objects.get(pk=instance.pk)
        self.assertEqual(readout.duration, timedelta(1,0))

    def test_none_saves_as_default(self):
        instance = DurationFieldModelWithDefault()
        # this could happen if we were reading an existing instance out of the database that didn't have this field
        instance.duration = None
        instance.save()

        readout = DurationFieldModelWithDefault.objects.get(pk=instance.pk)
        self.assertEqual(readout.duration, timedelta(1,0))


# ModelWithNonNullableFieldAndDefaultValueTests verifies that we maintain same
# behavior as Django with respect to a model field that is non-nullable with default value.
class ModelWithNonNullableFieldAndDefaultValueTests(TestCase):

    def _create_instance_with_null_field_value(self):

        instance = ModelWithNonNullableFieldAndDefaultValue.objects.create(some_field=1)

        from django.db import connections
        gclient = connections.all()[0].connection.gclient

        key = gclient.key(
            ModelWithNonNullableFieldAndDefaultValue._meta.db_table,
            instance.pk
        )

        entity = gclient.get(key)

        del entity["some_field"]

        gclient.put(entity)

        instance.refresh_from_db()

        return instance

    @disable_cache()
    def test_none_in_db_reads_as_none_in_model(self):

        instance = self._create_instance_with_null_field_value()

        self.assertIsNone(instance.some_field)

    @disable_cache()
    def test_none_in_model_saved_as_default(self):

        instance = self._create_instance_with_null_field_value()

        instance.save()
        instance.refresh_from_db()

        self.assertEqual(instance.some_field, 1086)
        
