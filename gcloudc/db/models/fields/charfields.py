from django.core.exceptions import ImproperlyConfigured
from django.db import models

from gcloudc.core import validators


_MAX_STRING_LENGTH = 1500


class CharOrNoneField(models.CharField):
    """ A field that stores only non-empty strings or None (it won't store empty strings).
        This is useful if you want values to be unique but also want to allow empty values.
    """

    empty_strings_allowed = False

    def __init__(self, *args, **kwargs):
        # Don't allow null=False because that would be insane.
        if not kwargs.get("null", True):
            raise ImproperlyConfigured("You can't set null=False on a CharOrNoneField.")
        # Set blank=True as the default, but allow it to be overridden, as it's theoretically
        # possible that you might want to prevent emptiness only in a form
        defaults = dict(null=True, blank=True, default=None)
        defaults.update(**kwargs)
        super(CharOrNoneField, self).__init__(*args, **defaults)

    def pre_save(self, model_instance, add):
        value = super(CharOrNoneField, self).pre_save(model_instance, add)
        # Change empty strings to None
        if not value:
            return None
        return value


class CharField(models.CharField):
    def __init__(self, max_length=_MAX_STRING_LENGTH, *args, **kwargs):
        assert max_length <= _MAX_STRING_LENGTH, "%ss max_length must not be greater than %d bytes." % (
            self.__class__.__name__,
            _MAX_STRING_LENGTH,
        )

        super(CharField, self).__init__(max_length=max_length, *args, **kwargs)

        # Append the MaxBytesValidator if it's not been included already
        self.validators = [x for x in self.validators if not isinstance(x, validators.MaxBytesValidator)] + [
            validators.MaxBytesValidator(limit_value=max_length)
        ]
