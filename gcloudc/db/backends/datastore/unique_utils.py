from hashlib import md5

from django.conf import settings

from . import meta_queries


def _has_enabled_constraints(model_or_instance):
    """
    Takes into account any model or global level flag which would determine
    if we should respect any unique constraints defined on a model definition.
    """
    # are unique constraints explicitly enabled on the provided model take
    # precident over any contradictory global flag
    # TODO need to change Djangae ref for something else...
    opts = getattr(model_or_instance, "Djangae", None)
    if opts:
        if hasattr(opts, "enforce_constraint_checks"):
            if opts.enforce_constraint_checks:
                return True

    # are unique constraints active/disabled via the global flag
    return getattr(settings, "ENFORCE_CONSTRAINT_CHECKS", True)


def _has_unique_constraints(model_or_instance):
    """
    Returns a boolean to indicate if the given model has any type of unique
    constraint defined (e.g. unique on a single field, or meta.unique_together).

    Note - you can see a much more verbose implementation of this in
    django.db.models.base.Model._get_unique_checks() - but we implement our
    own logic to exit early when the first constraint is found.
    """
    meta_options = model_or_instance._meta

    unique_fields = meta_options.unique_together
    unique_together = any(field.unique for field in meta_options.fields)

    return any([unique_fields, unique_together])


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


def unique_identifiers_from_entity(model, entity, ignore_pk=True, ignore_null_values=True):
    """
    This method returns a list of all unique marker key identifier values for
    the given entity by combining the field and entity values. For example:

    [
        # example of a single field unique constraint
        djange_<model_db_table>|<field_name>:<entity_value>
        # example of unique together
        djange_<model_db_table>|<field_name>:<entity_value>|<field_name>:<entity_value>
        ...
    ]

    These are then used before we put() anything into the database, to check
    that there are no existing markers satisfying those unique constraints.
    """
    from .utils import get_top_concrete_parent

    # get all combintatons of unique combinations defined on the model class
    unique_combinations = _unique_combinations(model, ignore_pk)

    meta = model._meta

    identifiers = []
    for combination in unique_combinations:
        combo_identifiers = [[]]

        include_combination = True

        for field_name in combination:
            field = meta.get_field(field_name)

            if field.primary_key:
                value = entity.key.id_or_name
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
            # create the final value - eg <app_db_table>|<field_name>:<field_val>
            for identifier_pairs in combo_identifiers:
                constraint_prefix = get_top_concrete_parent(model)._meta.db_table
                constraint_suffix = "|".join(identifier_pairs)
                constraint_value = "{prefix}|{suffix}".format(
                    prefix=constraint_prefix, suffix=constraint_suffix
                )
                identifiers.append(constraint_value)

    return identifiers


def _get_kind_from_named_marker_key(unique_marker_key):
    """
    Extracts and returns the table name prefix from a UniqueMarker named key.
    """
    return unique_marker_key.name.split("|")[0]


def _get_unique_fields_from_named_marker_key(unique_marker_key):
    """
    Extracts and returns all the unique field names from a UniqueMarker
    named key.
    """
    fields_and_values = unique_marker_key.name.split("|")[1:]
    return [
        unique_field_name.split(":")[0] for
        unique_field_name in fields_and_values
    ]


def _unique_combinations(model, ignore_pk=False):
    """
    Returns an iterable of iterables to represent all the unique constraints
    defined on the model. For example given the following model definition:

        class ExampleModel(models.Model):
            username = models.CharField(unique=True)
            email = models.EmailField(primary_key=True)
            first_name = models.CharField()
            second_name = models.CharField()

        class Meta:
            unique_together = ('first_name', 'second_name')

    This method would return

    [
        ['username'], # from field level unique=True
        ['email'], # implicit unique constraint from primary_key=True
        ['first_name', 'second_name'] # from model meta unique_together
    ]
    """
    # first grab all the unique together constraints
    unique_constraints = [
        list(together_constraint) for
        together_constraint in model._meta.unique_together
    ]

    # then the column level constraints - special casing PK if required
    for field in model._meta.fields:
        if field.primary_key and ignore_pk:
            continue

        if field.unique:
            unique_constraints.append([field.name])

    # the caller should sort each inner iterable really - but we do this here
    # for now - motive being that interpolated keys from these values are consistent
    return [sorted(constraint) for constraint in unique_constraints]


def _format_value_for_identifier(value):
    # AppEngine max key length is 500 chars, so if the value is a string we hexdigest it to reduce the length
    # otherwise we str() it as it's probably an int or bool or something.
    return (
        md5(value.encode("utf-8")).hexdigest() if
        isinstance(value, str) else
        str(value)
    )

