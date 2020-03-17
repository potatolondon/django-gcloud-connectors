# encoding: utf-8
import logging
import os
import zipfile

from django.db import models

from .charfields import CharField


# pyuca only supports version 5.2.0 of the collation algorithm on Python 2.x
COLLATION_FILE = "allkeys-5.2.0.txt"
COLLATION_ZIP_FILE = os.path.join(os.path.dirname(__file__), "allkeys-5.2.0.zip")

logger = logging.getLogger(__file__)


class ZipLoaderMixin(object):
    """
        The UCA collation file is massive (nearly 1.5M) but it's all text
        so it compresses easily. We ship the file zipped up and then decompress
        it on the fly here to save on storage, data transfer, memory etc.
        The use of generators on load should be efficient.
    """

    def __init__(self, zip_filename, text_filename):
        """
            The BaseCollator class __init__ takes a filename and calls
            load(filename). Here we pass up the text filename but store the
            zip filename, then override load so we can load from the zip instead
            of a filesystem.
        """
        self.zip_filename = zip_filename
        super(ZipLoaderMixin, self).__init__(filename=text_filename)

    def load(self, filename):
        from pyuca.collator import COLL_ELEMENT_PATTERN, hexstrings2int  # pyuca is required for ComputedCollationField

        with zipfile.ZipFile(self.zip_filename) as z:
            with z.open(filename) as f:
                for line in f.readlines():
                    line = line.decode("utf-8")
                    line = line.split("#", 1)[0].rstrip()

                    if not line or line.startswith("@version"):
                        continue

                    a, b = line.split(";", 1)
                    char_list = hexstrings2int(a.split())
                    coll_elements = []
                    for x in COLL_ELEMENT_PATTERN.finditer(b.strip()):
                        weights = x.groups()
                        coll_elements.append(hexstrings2int(weights))
                    self.table.add(char_list, coll_elements)


class ComputedFieldMixin:
    def __init__(self, func, *args, **kwargs):
        self.computer = func

        kwargs["editable"] = False

        super().__init__(*args, **kwargs)

    def pre_save(self, model_instance, add):
        value = self.get_computed_value(model_instance)
        setattr(model_instance, self.attname, value)
        return value

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        args = [self.computer] + args
        del kwargs["editable"]
        return name, path, args, kwargs

    def from_db_value(self, value, expression, connection, context):
        if value is None:
            return value
        return self.to_python(value)

    def get_computed_value(self, model_instance):
        # self.computer is either a function or a string containing the name of a method
        if callable(self.computer):
            return self.computer(model_instance)
        else:
            computer = getattr(model_instance, self.computer)
            return computer()


class ComputedCharField(ComputedFieldMixin, models.CharField):
    pass


class ComputedIntegerField(ComputedFieldMixin, models.IntegerField):
    pass


class ComputedTextField(ComputedFieldMixin, models.TextField):
    pass


class ComputedPositiveIntegerField(ComputedFieldMixin, models.PositiveIntegerField):
    pass


class ComputedBooleanField(ComputedFieldMixin, models.BooleanField):
    pass


class ComputedNullBooleanField(ComputedFieldMixin, models.NullBooleanField):
    pass


class ComputedCollationField(ComputedFieldMixin, CharField):
    """
        App Engine sorts strings based on the unicode codepoints that make them
        up. When you have strings from non-ASCII languages this makes the sort order
        incorrect (e.g. Ł will be sorted after Z).
        This field uses the pyuca library to calculate a sort key using the
        Unicode Collation Algorithm, which can then be used for ordering querysets
        correctly.
    """

    collator = None

    def __init__(self, source_field_name):
        import pyuca  # noqa: F401 Required dependency for ComputedCollationField
        from pyuca.collator import Collator_5_2_0

        # Instantiate Collator once only to save on memory / processing
        if not ComputedCollationField.collator:

            class Collator(ZipLoaderMixin, Collator_5_2_0):
                pass

            ComputedCollationField.collator = Collator(COLLATION_ZIP_FILE, COLLATION_FILE)

        def truncate(unicode_str):
            encoded = unicode_str.encode("utf-8")[:1500]
            # We ignore unrecognized chars as the truncation might
            # have split a unicode char down the middle
            return encoded.decode("utf-8", "ignore")

        def computer(instance):
            source_value = getattr(instance, source_field_name) or u""
            if not isinstance(source_value, str):
                source_value = str(source_value, "utf-8")
            sort_key = self.collator.sort_key(source_value)
            sort_key = u"".join([chr(x) for x in sort_key])
            truncated_key = truncate(sort_key)
            if truncated_key != sort_key:
                logger.warn("Truncated sort key for '%s.%s'", instance._meta.db_table, source_field_name)
            return truncated_key

        super(ComputedCollationField, self).__init__(computer)

    def deconstruct(self):
        name, path, args, kwargs = super(ComputedCollationField, self).deconstruct()
        del kwargs["max_length"]
        return name, path, args, kwargs
