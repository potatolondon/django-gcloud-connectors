from django.core.exceptions import NON_FIELD_ERRORS

from .dbapi import NotSupportedError


MAX_ALLOWABLE_QUERIES = 30


class UniquenessMixin(object):
    """
    Mixin overriding the default methods checking value uniqueness (the logic
    if a copy of the Django implementation, aside from the part marked by
    comments).

    This must be used if you have a list or set field marked as unique.
    """

    def _perform_unique_checks(self, unique_checks):
        errors = {}
        for model_class, unique_check in unique_checks:
            lookup_kwargs = {}
            for field_name in unique_check:
                f = self._meta.get_field(field_name)
                lookup_value = getattr(self, f.attname)
                if lookup_value is None:
                    continue
                if f.primary_key and not self._state.adding:
                    continue

                ##########################################################################
                # This is a modification to Django's native implementation of this method;
                # we conditionally build a __in lookup if the value is an iterable.
                lookup = str(field_name)
                if isinstance(lookup_value, (list, set, tuple)):
                    lookup = "%s__overlap" % lookup

                lookup_kwargs[lookup] = lookup_value
                ##########################################################################
                # / end of changes

            if len(unique_check) != len(lookup_kwargs):
                continue

            #######################################################
            # Deal with long __in lookups by doing multiple queries in that case
            # This is a bit hacky, but we really have no choice due to App Engine's
            # 30 multi-query limit. This also means we can't support multiple list fields in
            # a unique combination
            #######################################################

            if len([x for x in lookup_kwargs if x.endswith("__in")]) > 1:
                raise NotSupportedError("You cannot currently have two list fields in a unique combination")

            # Split IN queries into multiple lookups if they are too long
            lookups = []
            for k, v in lookup_kwargs.items():
                if (k.endswith("__in") or k.endswith("__overlap")) and len(v) > MAX_ALLOWABLE_QUERIES:
                    v = list(v)
                    while v:
                        new_lookup = lookup_kwargs.copy()
                        new_lookup[k] = v[:30]
                        v = v[30:]
                        lookups.append(new_lookup)
                    break
            else:
                # Otherwise just use the one lookup
                lookups = [lookup_kwargs]

            for lookup_kwargs in lookups:
                qs = model_class._default_manager.filter(**lookup_kwargs).values_list("pk", flat=True)
                model_class_pk = self._get_pk_val(model_class._meta)
                result = list(qs)

                if not self._state.adding and model_class_pk is not None:
                    # If we are saving an instance, we ignore it's PK in the result
                    try:
                        result.remove(model_class_pk)
                    except ValueError:
                        pass

                if result:
                    if len(unique_check) == 1:
                        key = unique_check[0]
                    else:
                        key = NON_FIELD_ERRORS
                    errors.setdefault(key, []).append(self.unique_error_message(model_class, unique_check))
                    break
        return errors
