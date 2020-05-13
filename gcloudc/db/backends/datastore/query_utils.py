"""
    Utility functions for gathering data from Google Datastore Query
    objects.
"""


def has_filter(query, col_and_operator):
    """
        query: A Cloud Datastore Query object
        col_and_operator: tuple of column name and operator
    """
    for col, operator, value in query.filters:
        if (col, operator) == tuple(col_and_operator):
            return True

    return False


def get_filter(query, col_and_operator):
    """
        query: A Cloud Datastore Query object
        col_and_operator: tuple of column name and operator
    """
    for col, operator, value in query.filters:
        if (col, operator) == tuple(col_and_operator):
            return value

    return None


def is_keys_only(query):
    return query.projection == ["__key__"]


def compare_keys(lhs, rhs):
    """
        The App Engine API used to provide a key comparison, but for
        some reason the Cloud Datastore API doesn't :(
    """

    def cmp(a, b):
        if a is None and b is None:
            return 0

        if a is None:
            return -1

        if b is None:
            return 1

        return (a > b) - (a < b)

    lhs_args = [lhs.project, lhs.namespace] + list(lhs.flat_path)
    if lhs.is_partial:
        # If the key is partial, then we need to add a blank placeholder
        # for the id or name so we can compare correctly
        lhs_args.extend("")

    rhs_args = [rhs.project, rhs.namespace] + list(rhs.flat_path)
    if rhs.is_partial:
        rhs_args.extend("")

    for lhs_component, rhs_component in zip(lhs_args, rhs_args):
        comparison = cmp(lhs_component, rhs_component)
        if comparison != 0:
            return comparison

    return cmp(len(lhs_args), len(rhs_args))
