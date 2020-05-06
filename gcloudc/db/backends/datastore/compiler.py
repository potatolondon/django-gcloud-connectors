# LIBRARIES
from django.db.models.expressions import (
    OrderBy,
    Value,
)
from django.db.models.sql import compiler
from django.db.models.sql.query import get_order_dir

# GCLOUDC
from .commands import (
    DeleteCommand,
    InsertCommand,
    SelectCommand,
    UpdateCommand,
)


class SQLCompiler(compiler.SQLCompiler):
    def find_ordering_name(self, name, opts, alias=None, default_order="ASC", already_seen=None):
        """
            Overridden just for the __scatter__ property ordering
        """

        # This allow special appengine properties (e.g. __scatter__) to be supplied as an ordering
        # even though they don't (and can't) exist as Django model fields
        if name.startswith("__") and name.endswith("__"):
            name, order = get_order_dir(name, default_order)
            descending = True if order == "DESC" else False
            return [(OrderBy(Value("__scatter__"), descending=descending), False)]

        return super(SQLCompiler, self).find_ordering_name(
            name, opts, alias=alias, default_order=default_order, already_seen=already_seen
        )

    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        self.pre_sql_setup()
        self.refcounts_before = self.query.alias_refcount.copy()

        select = SelectCommand(self.connection, self.query)
        return (select, tuple())

    def get_select(self):
        self.query.select_related = False  # Make sure select_related is disabled for all queries
        return super(SQLCompiler, self).get_select()


class SQLInsertCompiler(SQLCompiler, compiler.SQLInsertCompiler):
    def __init__(self, *args, **kwargs):
        self.return_id = None
        super(SQLInsertCompiler, self).__init__(*args, **kwargs)

    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        self.pre_sql_setup()

        from gcloudc.db.backends.datastore.utils import get_concrete_fields

        # Always pass down all the fields on an insert
        return [
            (
                InsertCommand(
                    self.connection,
                    self.query.model,
                    self.query.objs,
                    list(self.query.fields) + list(get_concrete_fields(self.query.model, ignore_leaf=True)),
                    self.query.raw,
                ),
                tuple(),
            )
        ]


class SQLDeleteCompiler(SQLCompiler, compiler.SQLDeleteCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        return (DeleteCommand(self.connection, self.query), tuple())


class SQLUpdateCompiler(SQLCompiler, compiler.SQLUpdateCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        self.pre_sql_setup()
        return (UpdateCommand(self.connection, self.query), tuple())


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        if self.query.subquery:
            self.query.high_mark = self.query.subquery.query.high_mark
            self.query.low_mark = self.query.subquery.query.low_mark
        return SQLCompiler.as_sql(self, with_limits, with_col_aliases, subquery)
