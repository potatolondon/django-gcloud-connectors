from django.core.validators import (
    BaseValidator,
    MaxLengthValidator,
    MinLengthValidator,
)
from django.utils.deconstruct import deconstructible
from django.utils.translation import ungettext_lazy


class MaxBytesValidator(BaseValidator):
    def compare(self, a, b):
        return a > b

    def clean(self, x):
        return len(x.encode("utf-8"))

    message = ungettext_lazy(
        "Ensure this value has at most %(limit_value)d byte (it has %(show_value)d).",
        "Ensure this value has at most %(limit_value)d bytes (it has %(show_value)d).",
        "limit_value",
    )
    code = "max_length"


@deconstructible
class MinItemsValidator(MinLengthValidator):
    """ Copy of MinLengthValidator, but with the message customised to say "items" instead of
        "characters".
    """

    message = ungettext_lazy(
        "Ensure this field has at least %(limit_value)d item (it has %(show_value)d).",
        "Ensure this field has at least %(limit_value)d items (it has %(show_value)d).",
        "limit_value",
    )


@deconstructible
class MaxItemsValidator(MaxLengthValidator):
    """ Copy of MaxLengthValidator, but with the message customised to say "items" instead of
        "characters".
    """

    message = ungettext_lazy(
        "Ensure this field has at most %(limit_value)d item (it has %(show_value)d).",
        "Ensure this field has at most %(limit_value)d items (it has %(show_value)d).",
        "limit_value",
    )
