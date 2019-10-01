"""
JSONField automatically serializes most Python terms to JSON data.
Creates a TEXT field with a default value of "{}".  See test_json.py for
more information.

 from django.db import models
 from django_extensions.db.fields import json

 class LOL(models.Model):
     extra = json.JSONField()

This field originated from the django_extensions project: https://github.com/django-extensions/django-extensions
"""

from __future__ import absolute_import

import json
from collections import OrderedDict

from django.db import models
from django.conf import settings
from django.utils import six
from django.core.serializers.json import DjangoJSONEncoder

from gcloudc.db.backends.datastore.indexing import Indexer, register_indexer, IgnoreForIndexing
from gcloudc.forms.fields import JSONFormField, JSONWidget

__all__ = ("JSONField",)


def dumps(value):
    return DjangoJSONEncoder().encode(value)


def loads(txt, object_pairs_hook=None):
    value = json.loads(txt, encoding=settings.DEFAULT_CHARSET, object_pairs_hook=object_pairs_hook)
    return value


class JSONDict(dict):
    """
    Hack so repr() called by dumpdata will output JSON instead of
    Python formatted data.  This way fixtures will work!
    """

    def __repr__(self):
        return dumps(self)


class JSONUnicode(six.text_type):
    """
    As above
    """

    def __repr__(self):
        return dumps(self)


class JSONList(list):
    """
    As above
    """

    def __repr__(self):
        return dumps(self)


class JSONOrderedDict(OrderedDict):
    """
    As above
    """

    def __repr__(self):
        return dumps(self)


class JSONKeyLookup(models.Lookup):
    lookup_name = "json_path"
    operator = "json_path"
    bilateral_transforms = False
    lookup_supports_text = True  # Tell Djangae connector that we are OK on text fields

    def __init__(self, path):
        self.path = path

    def get_rhs_op(self, connection, rhs):
        return "__".join([self.operator, self.path])

    def __call__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs
        return self


class JSONField(models.TextField):
    """JSONField is a generic textfield that neatly serializes/unserializes
    JSON objects seamlessly.  Main thingy must be a dict object."""

    def __init__(self, use_ordered_dict=False, *args, **kwargs):
        if "default" in kwargs:
            if not callable(kwargs["default"]):
                raise TypeError("'default' must be a callable (e.g. 'dict' or 'list')")
        else:
            kwargs["default"] = dict

        # use `collections.OrderedDict` rather than built-in `dict`
        self.use_ordered_dict = use_ordered_dict

        models.TextField.__init__(self, *args, **kwargs)

    def parse_json(self, value):
        """Convert our string value to JSON after we load it from the DB"""
        if value is None or value == "":
            return {}
        elif isinstance(value, six.string_types):
            try:
                if self.use_ordered_dict:
                    res = loads(value, object_pairs_hook=OrderedDict)
                else:
                    res = loads(value)
            except ValueError:
                # If we can't parse as JSON, just return the value as-is
                return value

            if isinstance(res, OrderedDict) and self.use_ordered_dict:
                return JSONOrderedDict(res)
            elif isinstance(res, dict):
                return JSONDict(**res)
            elif isinstance(res, six.string_types):
                return JSONUnicode(res)
            elif isinstance(res, list):
                return JSONList(res)
            return res
        else:
            return value

    def to_python(self, value):
        return self.parse_json(value)

    def from_db_value(self, value, expression, connection, context):
        return self.parse_json(value)

    def value_to_string(self, obj):
        value = self.value_from_object(obj)

        if isinstance(value, str):
            return value

        return dumps(value)

    def get_db_prep_save(self, value, connection, **kwargs):
        """Convert our JSON object to a string before we save"""
        if value is None and self.null:
            return None

        return super(JSONField, self).get_db_prep_save(dumps(value), connection=connection)

    def south_field_triple(self):
        """Returns a suitable description of this field for South."""
        # We'll just introspect the _actual_ field.
        from south.modelsinspector import introspector

        field_class = "django.db.models.fields.TextField"
        args, kwargs = introspector(self)
        # That's our definition!
        return (field_class, args, kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(JSONField, self).deconstruct()
        if self.default == {}:
            del kwargs["default"]
        return name, path, args, kwargs

    def formfield(self, **kwargs):
        defaults = {"form_class": JSONFormField, "widget": JSONWidget}
        defaults.update(kwargs)
        return super(JSONField, self).formfield(**defaults)

    def get_lookup(self, lookup):
        ret = super(JSONField, self).get_lookup(lookup)
        if ret:
            return ret
        return JSONKeyLookup(lookup)

    def get_transform(self, lookup):
        ret = super(JSONField, self).get_transform(lookup)
        if ret:
            return ret

        # Assume a key path lookup
        class LookupBuilder(models.Transform):
            source_expressions = []
            lookup_name = "json_path"

            def __init__(self, *args, **kwargs):
                super(LookupBuilder, self).__init__(*args, **kwargs)
                self.path = [lookup]

            def get_transform(self, lookup):
                self.path.append(lookup)
                return lambda lhs, *args, **kwargs: self

            def get_lookup(self, lookup):
                self.path.append(lookup)
                return JSONKeyLookup("__".join(self.path))

        return LookupBuilder


class JSONKeyLookupIndexer(Indexer):
    OPERATOR = "json_path"

    def handles(self, field, operator):
        from gcloudc.db.models.fields.json import JSONField

        operator_part = operator.split("__", 1)[0]
        return isinstance(field, JSONField) and operator_part == self.OPERATOR

    def prepare_index_type(self, index_type, value):
        return index_type

    def prep_value_for_query(self, value, **kwargs):
        return value

    def prep_query_operator(self, op):
        return "exact"

    def prep_value_for_database(self, value, index, **kwargs):
        if isinstance(value, six.string_types):
            value = json.loads(value)

        index_part = index.split("__", 1)[1]
        path = index_part.split("__")

        is_isnull = False
        # Ignore isnull on the end of a path, it's not a value lookup
        if len(path) > 1 and path[-1] == "isnull":
            is_isnull = True
            path.pop()

        # Go through the path and look up the value from each
        # dictionary/list if we fail to find a value we raise
        # a IgnoreForIndexing exception which tells the special indexer
        # to not save *anything*
        for i, section in enumerate(path):
            try:
                section = int(section)
            except (TypeError, ValueError):
                pass

            try:
                value = value[section]
            except (KeyError, IndexError, TypeError):
                raise IgnoreForIndexing("")

        if is_isnull:
            return value is None

        return value

    def indexed_column_name(self, field_column, value, index):
        return "_idx_json_path_{}_{}".format(field_column, index.split("__", 1)[-1])


# Especially for JSON fields
register_indexer(JSONKeyLookupIndexer)
