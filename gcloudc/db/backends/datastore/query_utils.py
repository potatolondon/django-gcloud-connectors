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
