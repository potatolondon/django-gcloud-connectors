from collections import OrderedDict

from django.db import (
    connection,
    models,
)
from gcloudc.db.models.fields.json import JSONField
from google.cloud.datastore.entity import Entity

from . import TestCase


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
        bool(JSONFieldModel.objects.exists())  # Force a connection (Django might not have made one yet)

        client = connection.connection.gclient
        entity = Entity(
            client.key(JSONFieldModel._meta.db_table, 1, namespace=connection.settings_dict["NAMESPACE"])
        )
        entity["json_field"] = "bananas"
        client.put(entity)

        instance = JSONFieldModel.objects.get(pk=1)
        self.assertEqual(instance.json_field, "bananas")

    def test_object_pairs_hook_with_ordereddict(self):
        items = [("first", 1), ("second", 2), ("third", 3), ("fourth", 4)]
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
        field = JSONFieldModel._meta.get_field("json_field")
        field.use_ordered_dict = False

        normal_dict = {"a": 1, "b": 2, "c": 3}

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
        test_instance = JSONFieldModel(json_field={"test": 0.1})
        test_instance.save()

        test_instance = JSONFieldModel.objects.get()
        test_instance.save()

        test_instance = JSONFieldModel.objects.get()
        self.assertEqual(test_instance.json_field["test"], 0.1)

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
