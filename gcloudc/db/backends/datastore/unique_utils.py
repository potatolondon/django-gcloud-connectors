from hashlib import md5

from . import meta_queries


def _unique_combinations(model, ignore_pk=False):
    unique_names = [ [ model._meta.get_field(y).name for y in x ] for x in model._meta.unique_together ]

    for field in model._meta.fields:
        if field.primary_key and ignore_pk:
            continue

        if field.unique:
            unique_names.append([field.name])

    return [ sorted(x) for x in unique_names ]


def _format_value_for_identifier(value):
    # AppEngine max key length is 500 chars, so if the value is a string we hexdigest it to reduce the length
    # otherwise we str() it as it's probably an int or bool or something.
    return md5(value.encode("utf-8")).hexdigest() if isinstance(value, basestring) else str(value)


def unique_identifiers_from_entity(model, entity, ignore_pk=False, ignore_null_values=True):
    """
        Given an instance, this function returns a list of identifier strings that represent
        unique field/value combinations.
    """
    from djangae.db.utils import get_top_concrete_parent

    unique_combinations = _unique_combinations(model, ignore_pk)

    meta = model._meta

    identifiers = []
    for combination in unique_combinations:
        combo_identifiers = [[]]

        include_combination = True

        for field_name in combination:
            field = meta.get_field(field_name)

            if field.primary_key:
                value = entity.key().id_or_name()
            else:
                value = entity.get(field.column)  # Get the value from the entity

            # If ignore_null_values is True, then we don't include combinations where the value is None
            # or if the field is a multivalue field where None means no value (you can't store None in a list)
            if (value is None and ignore_null_values) or (not value and isinstance(value, (list, set))):
                include_combination = False
                break

            if not isinstance(value, (list, set)):
                value = [value]

            new_combo_identifers = []

            for existing in combo_identifiers:
                for v in value:
                    identifier = "{}:{}".format(field.column, _format_value_for_identifier(v))
                    new_combo_identifers.append(existing + [identifier])

            combo_identifiers = new_combo_identifers

        if include_combination:
            for ident in combo_identifiers:
                identifiers.append(get_top_concrete_parent(model)._meta.db_table + "|" + "|".join(ident))

    return identifiers


def query_is_unique(model, query):
    """
        If the query is entirely on unique constraints then return the unique identifier for
        that unique combination. Otherwise return False
    """

    if isinstance(query, meta_queries.AsyncMultiQuery):
        # By definition, a multiquery is not unique
        return False

    combinations = _unique_combinations(model)

    queried_fields = [x[0] for x in query.filters]

    for combination in combinations:
        unique_match = True
        field_names = []
        for field in combination:
            if field == model._meta.pk.column:
                field = "__key__"
            else:
                field = model._meta.get_field(field).column
            field_names.append(field)

            # We don't match this combination if the field didn't exist in the queried fields
            # or if it was, but the value was None (you can have multiple NULL values, they aren't unique)
            key = "{} =".format(field)
            if key not in queried_fields or query[key] is None:
                unique_match = False
                break

        if unique_match:
            return "|".join([model._meta.db_table] + [
                "{}:{}".format(x, _format_value_for_identifier(query["{} =".format(x)]))
                for x in field_names
            ])

    return False
