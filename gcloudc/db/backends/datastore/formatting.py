from __future__ import unicode_literals

import json

from django.utils import six

SELECT_PATTERN = """
SELECT (%(columns)s) FROM %(table)s
WHERE %(where)s
ORDER BY %(order)s
OFFSET %(offset)s
LIMIT %(limit)s
""".lstrip()

INSERT_PATTERN = """
INSERT INTO %(table)s (%(columns)s)
VALUES %(values)s
""".lstrip()

UPDATE_PATTERN = """
REPLACE INTO %(table)s (%(columns)s)
VALUES %(values)s
WHERE %(where)s
""".lstrip()

DELETE_PATTERN = """
DELETE FROM %(table)s
WHERE %(where)s
""".lstrip()


def _generate_values_expression(instances, columns):
    values = []
    for instance in instances:
        row = []
        for column in columns:
            # FIXME: should this use get_default as a default?
            value = getattr(instance, column, None)

            try:
                text_value = _quote_string(value)
            except UnicodeDecodeError:
                text_value = "'<binary>'"

            row.append(text_value)

        values.append("(" + ", ".join(row) + ")")
    return ", ".join(values)


def _generate_insert_sql(command):
    columns = sorted([x.column for x in command.fields])

    params = {
        "table": command.model._meta.db_table,
        "columns": ", ".join(columns),
        "values": _generate_values_expression(command.objs, columns),
    }

    return (INSERT_PATTERN % params).replace("\n", " ").strip()


def _generate_where_expression(representation):
    where = []
    for branch in representation["where"]:
        branch = "(" + " AND ".join(["%s%s" % (k, _quote_string(v)) for k, v in branch.items()]) + ")"
        where.append(branch)

    return " OR ".join(where)


def _quote_string(value):
    needs_quoting = isinstance(value, six.string_types)
    # in ANSI SQL as well as GQL, string literals are wrapped in single quotes
    return "'{}'".format(value) if needs_quoting else six.text_type(value)


def _generate_select_sql(command, representation):
    has_offset = representation["low_mark"] > 0
    has_limit = representation["high_mark"] is not None
    has_ordering = bool(representation["order_by"])
    has_where = bool(representation["where"])

    lines = SELECT_PATTERN.split("\n")

    # Remove limit and offset and where if we don't need them
    if not has_limit:
        del lines[4]

    if not has_offset:
        del lines[3]

    if not has_ordering:
        del lines[2]

    if not has_where:
        del lines[1]

    sql = "\n".join(lines)

    columns = (
        "*"
        if not representation["columns"]
        else ", ".join(sorted(representation["columns"]))  # Just to make the output predictable
    )

    ordering = [
        ("%s %s" % (x.lstrip("-"), "DESC" if x.startswith("-") else "")).strip() for x in representation["order_by"]
    ]

    replacements = {
        "table": representation["table"],
        "columns": columns,
        "offset": representation["low_mark"],
        "limit": (representation["high_mark"] or 0) - (representation["low_mark"] or 0),
        "where": _generate_where_expression(representation),
        "order": ", ".join(ordering),
    }

    return (sql % replacements).replace("\n", " ").strip()


def _generate_delete_sql(command, representation):
    has_where = bool(representation["where"])

    lines = DELETE_PATTERN.split("\n")
    if not has_where:
        del lines[1]

    sql = "\n".join(lines)

    return (
        (sql % {"table": representation["table"], "where": _generate_where_expression(representation)})
        .replace("\n", " ")
        .strip()
    )


def _generate_update_sql(command, representation):
    has_where = bool(representation["where"])

    lines = UPDATE_PATTERN.split("\n")
    if not has_where:
        del lines[2]

    sql = "\n".join(lines)
    columns = sorted([x[0].column for x in command.values])

    values = {x[0].column: six.text_type(x[2]) for x in command.values}

    params = {
        "table": representation["table"],
        "columns": ", ".join(columns),
        "values": "(" + ", ".join([values[x] for x in columns]) + ")",
        "where": _generate_where_expression(representation),
    }

    return (sql % params).replace("\n", " ").strip()


def generate_sql_representation(command):
    from .commands import SelectCommand, DeleteCommand, UpdateCommand, InsertCommand

    if isinstance(command, InsertCommand):
        # Inserts don't have a .query so we have to deal with them
        # separately
        return _generate_insert_sql(command)

    representation = json.loads(command.query.serialize())

    if isinstance(command, SelectCommand):
        return _generate_select_sql(command, representation)
    elif isinstance(command, DeleteCommand):
        return _generate_delete_sql(command, representation)
    elif isinstance(command, UpdateCommand):
        return _generate_update_sql(command, representation)

    raise NotImplementedError("Unrecognized query type")
