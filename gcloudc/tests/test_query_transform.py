

from django.db import connection as default_connection
from django.db import connections
from django.db.models.query import Q
from django.db.models.sql.datastructures import EmptyResultSet

from gcloudc.db.backends.datastore import transaction
from gcloudc.db.backends.datastore.dnf import normalize_query
from gcloudc.db.backends.datastore.query import (
    Query,
    WhereNode,
    transform_query,
)

from . import TestCase
from .models import (
    InheritedModel,
    Relation,
    TestUser,
    TransformTestModel,
)

DEFAULT_NAMESPACE = default_connection.ops.connection.settings_dict.get("NAMESPACE")


def find_children_containing_node(list_of_possible_children, column, op, value):
    for children in list_of_possible_children:
        for node in (children or []):
            if node.column == column and node.operator == op and node.value == value:
                return children


class TransformQueryTest(TestCase):

    def test_polymodel_filter_applied(self):
        query = transform_query(
            connections['default'],
            InheritedModel.objects.filter(field1="One").all().query
        )
        query.prepare()

        self.assertEqual(2, len(query.where.children))
        self.assertTrue(query.where.children[0].children[0].is_leaf)
        self.assertTrue(query.where.children[1].children[0].is_leaf)
        self.assertEqual("class", query.where.children[0].children[0].column)
        self.assertEqual("field1", query.where.children[1].children[0].column)

    def test_basic_query(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.all().query
        )

        self.assertEqual(query.model, TransformTestModel)
        self.assertEqual(query.kind, 'SELECT')
        self.assertEqual(query.tables, [TransformTestModel._meta.db_table])
        self.assertIsNone(query.where)

    def test_and_filter(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.filter(field1="One", field2="Two").all().query
        )

        self.assertEqual(query.model, TransformTestModel)
        self.assertEqual(query.kind, 'SELECT')
        self.assertEqual(query.tables, [TransformTestModel._meta.db_table])
        self.assertTrue(query.where)
        self.assertEqual(2, len(query.where.children))  # Two child nodes

    def test_exclude_filter(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.exclude(field1="One").all().query
        )

        self.assertEqual(query.model, TransformTestModel)
        self.assertEqual(query.kind, 'SELECT')
        self.assertEqual(query.tables, [TransformTestModel._meta.db_table])
        self.assertTrue(query.where)
        self.assertEqual(1, len(query.where.children))  # One child node
        self.assertTrue(query.where.children[0].negated)
        self.assertEqual(1, len(query.where.children[0].children))

    def test_ordering(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.filter(field1="One", field2="Two").order_by("field1", "-field2").query
        )

        self.assertEqual(query.model, TransformTestModel)
        self.assertEqual(query.kind, 'SELECT')
        self.assertEqual(query.tables, [TransformTestModel._meta.db_table])
        self.assertTrue(query.where)
        self.assertEqual(2, len(query.where.children))  # Two child nodes
        self.assertEqual(["field1", "-field2"], query.order_by)

    def test_projection(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.only("field1").query
        )

        self.assertItemsEqual(["id", "field1"], query.columns)

        query = transform_query(
            connections['default'],
            TransformTestModel.objects.values_list("field1").query
        )

        self.assertEqual(set(["field1"]), query.columns)

        query = transform_query(
            connections['default'],
            TransformTestModel.objects.defer("field1", "field4").query
        )

        self.assertItemsEqual(set(["id", "field2", "field3"]), query.columns)

    def test_no_results_returns_emptyresultset(self):
        self.assertRaises(
            EmptyResultSet,
            transform_query,
            connections['default'],
            TransformTestModel.objects.none().query
        )

    def test_offset_and_limit(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.all()[5:10].query
        )

        self.assertEqual(5, query.low_mark)
        self.assertEqual(10, query.high_mark)

    def test_isnull(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.filter(field3__isnull=True).all()[5:10].query
        )

        self.assertTrue(query.where.children[0].value)
        self.assertEqual("ISNULL", query.where.children[0].operator)

    def test_distinct(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.distinct("field2", "field3").query
        )

        self.assertTrue(query.distinct)
        self.assertEqual(query.columns, set(["field2", "field3"]))

        query = transform_query(
            connections['default'],
            TransformTestModel.objects.distinct().values("field2", "field3").query
        )

        self.assertTrue(query.distinct)
        self.assertEqual(query.columns, set(["field2", "field3"]))

    def test_order_by_pk(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.order_by("pk").query
        )

        self.assertEqual("__key__", query.order_by[0])

        query = transform_query(
            connections['default'],
            TransformTestModel.objects.order_by("-pk").query
        )

        self.assertEqual("-__key__", query.order_by[0])

    def test_reversed_ordering(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.order_by("pk").reverse().query
        )

        self.assertEqual("-__key__", query.order_by[0])

    def test_clear_ordering(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.order_by("pk").order_by().query
        )

        self.assertFalse(query.order_by)

    def test_projection_on_textfield_disabled(self):
        query = transform_query(
            connections['default'],
            TransformTestModel.objects.values_list("field4").query
        )

        self.assertFalse(query.columns)
        self.assertFalse(query.projection_possible)


class QueryNormalizationTests(TestCase):
    """
        The parse_dnf function takes a Django where tree, and converts it
        into a tree of one of the following forms:

        [ (column, operator, value), (column, operator, value) ] <- AND only query
        [ [(column, operator, value)], [(column, operator, value) ]] <- OR query, of multiple ANDs
    """

    def test_and_with_child_or_promoted(self):
        """
            Given the following tree:

                   AND
                  / | ﹨
                 A  B OR
                      / ﹨
                     C   D

             The OR should be promoted, so the resulting tree is

                      OR
                     /   ﹨
                   AND   AND
                  / | ﹨ / | ﹨
                 A  B C A B D
        """

        query = Query(TestUser, "SELECT")
        query.where = WhereNode('default')
        query.where.children.append(WhereNode('default'))
        query.where.children[-1].column = "A"
        query.where.children[-1].operator = "="
        query.where.children.append(WhereNode('default'))
        query.where.children[-1].column = "B"
        query.where.children[-1].operator = "="
        query.where.children.append(WhereNode('default'))
        query.where.children[-1].connector = "OR"
        query.where.children[-1].children.append(WhereNode('default'))
        query.where.children[-1].children[-1].column = "C"
        query.where.children[-1].children[-1].operator = "="
        query.where.children[-1].children.append(WhereNode('default'))
        query.where.children[-1].children[-1].column = "D"
        query.where.children[-1].children[-1].operator = "="

        query = normalize_query(query)

        self.assertEqual(query.where.connector, "OR")
        self.assertEqual(2, len(query.where.children))
        self.assertFalse(query.where.children[0].is_leaf)
        self.assertFalse(query.where.children[1].is_leaf)
        self.assertEqual(query.where.children[0].connector, "AND")
        self.assertEqual(query.where.children[1].connector, "AND")
        self.assertEqual(3, len(query.where.children[0].children))
        self.assertEqual(3, len(query.where.children[1].children))

    def test_and_queries(self):
        qs = TestUser.objects.filter(username="test").all()

        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        self.assertTrue(1, len(query.where.children))
        self.assertEqual(query.where.children[0].children[0].column, "username")
        self.assertEqual(query.where.children[0].children[0].operator, "=")
        self.assertEqual(query.where.children[0].children[0].value, "test")

        qs = TestUser.objects.filter(username="test", email="test@example.com")

        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        self.assertTrue(2, len(query.where.children[0].children))

        self.assertEqual(query.where.connector, "OR")
        self.assertEqual(query.where.children[0].connector, "AND")

        self.assertTrue(find_children_containing_node([query.where.children[0]], "username", "=", "test"))
        self.assertTrue(find_children_containing_node([query.where.children[0]], "email", "=", "test@example.com"))

        qs = TestUser.objects.filter(username="test").exclude(email="test@example.com")
        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        self.assertTrue(2, len(query.where.children[0].children))
        self.assertEqual(query.where.connector, "OR")
        self.assertEqual(query.where.children[0].connector, "AND")

        possible_children = (query.where.children[0], query.where.children[1])

        expected = find_children_containing_node(possible_children, "email", "<", "test@example.com")
        self.assertTrue(find_children_containing_node([expected], "username", "=", "test"))
        self.assertTrue(find_children_containing_node([expected], "email", "<", "test@example.com"))

        expected = find_children_containing_node(possible_children, "email", ">", "test@example.com")
        self.assertTrue(find_children_containing_node([expected], "username", "=", "test"))
        self.assertTrue(find_children_containing_node([expected], "email", ">", "test@example.com"))

        instance = Relation(pk=1)
        qs = instance.related_set.filter(headline__startswith='Fir')

        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        self.assertTrue(2, len(query.where.children[0].children))
        self.assertEqual(query.where.connector, "OR")
        self.assertEqual(query.where.children[0].connector, "AND")
        self.assertEqual(query.where.children[0].children[0].column, "relation_id")
        self.assertEqual(query.where.children[0].children[0].operator, "=")
        self.assertEqual(query.where.children[0].children[0].value, 1)
        self.assertEqual(query.where.children[0].children[1].column, "_idx_startswith_headline")
        self.assertEqual(query.where.children[0].children[1].operator, "=")
        self.assertEqual(query.where.children[0].children[1].value, u"Fir")

    def test_impossible_or_query(self):
        qs = TestUser.objects.filter(
            username="python").filter(
            Q(username__in=["ruby", "jruby"]) | (Q(username="php") & ~Q(username="perl"))
        )

        with self.assertRaises(EmptyResultSet):
            normalize_query(transform_query(
                connections['default'],
                qs.query
            ))

    def test_or_queries(self):
        qs = TestUser.objects.filter(
            first_name="python").filter(
            Q(username__in=["ruby", "jruby"]) | (Q(username="php") & ~Q(username="perl"))
        )

        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        # After IN and != explosion, we have...
        # (AND:
        #       (first_name='python',
        #        OR: (username='ruby', username='jruby',
        #             AND: (username='php',
        #                   AND: (username < 'perl', username > 'perl')
        #                  )
        #            )
        #        )
        # )

        # Working backwards,
        # AND: (username < 'perl', username > 'perl') can't be simplified
        #
        # AND: (username='php', AND: (username < 'perl', username > 'perl'))
        # can become
        # (OR: (AND: username = 'php', username < 'perl'), (AND: username='php', username > 'perl'))
        #
        # OR: (username='ruby', username='jruby',(OR: (AND: username = 'php', username < 'perl'),
        # (AND: username='php', username > 'perl')) can't be simplified
        #
        # (AND: (first_name='python', OR: (username='ruby', username='jruby',
        # (OR: (AND: username = 'php', username < 'perl'), (AND: username='php', username > 'perl'))
        # becomes...
        # (OR: (AND: first_name='python', username = 'ruby'), (AND: first_name='python', username='jruby'),
        #      (AND: first_name='python', username='php', username < 'perl')
        #      (AND: first_name='python', username='php', username > 'perl')

        self.assertTrue(4, len(query.where.children[0].children))

        self.assertEqual(query.where.children[0].connector, "AND")

        possible_children = [query.where.children[i] for i in range(4)]

        expected = find_children_containing_node(possible_children, "username", "=", "jruby")
        self.assertTrue(find_children_containing_node([expected], "first_name", "=", "python"))
        self.assertTrue(find_children_containing_node([expected], "username", "=", "jruby"))

        expected = find_children_containing_node(possible_children, "username", ">", "perl")
        self.assertTrue(find_children_containing_node([expected], "first_name", "=", "python"))
        self.assertTrue(find_children_containing_node([expected], "username", "=", "php"))
        self.assertTrue(find_children_containing_node([expected], "username", ">", "perl"))

        expected = find_children_containing_node(possible_children, "username", "<", "perl")
        self.assertTrue(find_children_containing_node([expected], "first_name", "=", "python"))
        self.assertTrue(find_children_containing_node([expected], "username", "=", "php"))
        self.assertTrue(find_children_containing_node([expected], "username", "<", "perl"))

        expected = find_children_containing_node(possible_children, "username", "=", "ruby")
        self.assertTrue(find_children_containing_node([expected], "first_name", "=", "python"))
        self.assertTrue(find_children_containing_node([expected], "username", "=", "ruby"))

        qs = TestUser.objects.filter(username="test") | TestUser.objects.filter(username="cheese")

        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        self.assertEqual(query.where.connector, "OR")
        self.assertEqual(2, len(query.where.children))
        self.assertTrue(query.where.children[0].is_leaf)
        self.assertTrue("cheese" in (query.where.children[0].value, query.where.children[1].value))
        self.assertTrue(query.where.children[1].is_leaf)
        self.assertTrue("test" in (query.where.children[0].value, query.where.children[1].value))

        qs = TestUser.objects.using("default").filter(username__in=set()).values_list('email')

        with self.assertRaises(EmptyResultSet):
            query = normalize_query(transform_query(
                connections['default'],
                qs.query
            ))

        qs = TestUser.objects.filter(
            username__startswith='Hello'
        ) | TestUser.objects.filter(username__startswith='Goodbye')

        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        self.assertEqual(2, len(query.where.children))
        self.assertEqual("_idx_startswith_username", query.where.children[0].column)
        self.assertTrue("Goodbye" in (query.where.children[0].value, query.where.children[1].value))
        self.assertEqual("_idx_startswith_username", query.where.children[1].column)
        self.assertTrue("Hello" in (query.where.children[0].value, query.where.children[1].value))

        qs = TestUser.objects.filter(pk__in=[1, 2, 3])
        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        rpc = transaction._rpc(default_connection.alias)

        self.assertEqual(3, len(query.where.children))
        self.assertEqual("__key__", query.where.children[0].column)
        self.assertEqual("__key__", query.where.children[1].column)
        self.assertEqual("__key__", query.where.children[2].column)
        self.assertEqual({
                rpc.key(TestUser._meta.db_table, 1),
                rpc.key(TestUser._meta.db_table, 2),
                rpc.key(TestUser._meta.db_table, 3),
            }, {
                query.where.children[0].value,
                query.where.children[1].value,
                query.where.children[2].value,
            }
        )

        qs = TestUser.objects.filter(pk__in=[1, 2, 3]).filter(username="test")
        query = normalize_query(transform_query(
            connections['default'],
            qs.query
        ))

        self.assertEqual(3, len(query.where.children))
        self.assertEqual("__key__", query.where.children[0].children[0].column)
        self.assertEqual("test", query.where.children[0].children[1].value)

        self.assertEqual("__key__", query.where.children[1].children[0].column)
        self.assertEqual("test", query.where.children[0].children[1].value)

        self.assertEqual("__key__", query.where.children[2].children[0].column)
        self.assertEqual("test", query.where.children[0].children[1].value)

        self.assertEqual({
                rpc.key(TestUser._meta.db_table, 1),
                rpc.key(TestUser._meta.db_table, 2),
                rpc.key(TestUser._meta.db_table, 3),
            }, {
                query.where.children[0].children[0].value,
                query.where.children[1].children[0].value,
                query.where.children[2].children[0].value,
            }
        )

    def test_removal_of_multiple_pk_equalities(self):
        """ Regression test for #1174/#1175.
            Make sure that we don't get an error when a query has multiple different equality
            filters on the PK.
        """
        query = TransformTestModel.objects.filter(pk=1).filter(pk=2).filter(pk=3)
        try:
            list(query)
        except ValueError:
            raise
            self.fail("ValueError raised when filtering on multiple different PK equalities")
