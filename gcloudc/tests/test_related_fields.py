import sleuth
from django import forms
from django.core import serializers
from django.core.exceptions import (
    ImproperlyConfigured,
    ValidationError,
)
from django.db.utils import IntegrityError
from gcloudc.db.models.fields.related import (
    RelatedListField,
    RelatedSetField,
)

from . import TestCase
from .models import (
    ISModel,
    ISOther,
    IterableFieldsWithValidatorsModel,
    IterableRelatedModel,
    IterableRelatedWithNonIntPkModel,
    ModelDatabaseA,
    ModelDatabaseB,
    RelatedListFieldRemoveDuplicatesModel,
    StringPkModel,
    RelatedCharFieldModel,
    Tag,
    GenericRelationModel,
    Post,
    RelationWithoutReverse,
    RelationWithOverriddenDbTable,
)


class RelatedListFieldRemoveDuplicatesForm(forms.ModelForm):
    class Meta:
        model = RelatedListFieldRemoveDuplicatesModel
        fields = ["related_list_field"]


class RelatedIterableFieldTests(TestCase):
    """ Combined tests for common RelatedListField and RelatedSetField tests. """

    multi_db = True

    def test_cannot_have_min_length_and_blank(self):
        """ Having min_length=X, blank=True doesn't make any sense, especially when you consider
            that django will skip the min_length check when the value (list/set)is empty.
        """
        self.assertRaises(ImproperlyConfigured, RelatedListField, ISModel, min_length=1, blank=True)
        self.assertRaises(ImproperlyConfigured, RelatedSetField, ISModel, min_length=1, blank=True)

    def test_related_list_field_set_field_min_max_lengths_valid(self):
        """ Test that when the min_legnth and max_length of a ListField and SetField are correct
            that no validation error is rasied.
        """
        others = []
        for x in range(2):
            others.append(ISOther.objects.create())
        instance = IterableFieldsWithValidatorsModel(
            list_field=["1", "2"],  # not being tested here
            set_field=set(["1", "2"]),  # not being tested here
            related_set=set(others),
            related_list=others,
        )
        instance.full_clean()

    def test_related_list_field_max_length_invalid(self):
        others = []
        for x in range(5):
            others.append(ISOther.objects.create())
        instance = IterableFieldsWithValidatorsModel(
            list_field=["1", "2"],  # not being tested here
            set_field=set(["1", "2"]),  # not being tested here
            related_set=set(others[:2]),  # not being tested here
            related_list=others,
        )
        self.assertRaisesMessage(
            ValidationError,
            "{'related_list': ['Ensure this field has at most 3 items (it has 5).']}",
            instance.full_clean,
        )

    def test_related_list_field_min_length_invalid(self):
        others = []
        for x in range(2):
            others.append(ISOther.objects.create())
        instance = IterableFieldsWithValidatorsModel(
            list_field=["1", "2"],  # not being tested here
            set_field=set(["1", "2"]),  # not being tested here
            related_set=set(others),
            related_list=others[:1],
        )
        self.assertRaisesMessage(
            ValidationError,
            "{'related_list': ['Ensure this field has at least 2 items (it has 1).']}",
            instance.full_clean,
        )

    def test_related_set_field_max_length_invalid(self):
        others = []
        for x in range(5):
            others.append(ISOther.objects.create())
        instance = IterableFieldsWithValidatorsModel(
            list_field=["1", "2"],  # not being tested here
            set_field=set(["1", "2"]),  # not being tested here
            related_list=others[:2],  # not being tested here
            related_set=set(others),
        )
        self.assertRaisesMessage(
            ValidationError,
            "{'related_set': ['Ensure this field has at most 3 items (it has 5).']}",
            instance.full_clean,
        )

    def test_related_set_field_min_length_invalid(self):
        others = []
        for x in range(2):
            others.append(ISOther.objects.create())
        instance = IterableFieldsWithValidatorsModel(
            list_field=["1", "2"],  # not being tested here
            set_field=set(["1", "2"]),  # not being tested here
            related_list=others,  # not being tested here
            related_set=set(others[:1]),
        )
        self.assertRaisesMessage(
            ValidationError,
            "{'related_set': ['Ensure this field has at least 2 items (it has 1).']}",
            instance.full_clean,
        )

    def test_model_stores_ids_as_integers_when_saving(self):
        others = []
        for x in range(2):
            others.append(ISOther.objects.create())

        instance = IterableRelatedModel(
            related_set_ids=[str(x.pk) for x in others], related_list_ids=[str(x.pk) for x in others]
        )

        instance.save()
        instance.refresh_from_db()

        self.assertEqual(instance.related_set_ids, [int(x.pk) for x in others])
        self.assertEqual(instance.related_list_ids, [int(x.pk) for x in others])

    def test_model_stores_ids_as_non_integers(self):
        others = []
        for x in range(2):
            others.append(StringPkModel.objects.create(name=str(x)))

        instance = IterableRelatedWithNonIntPkModel(
            related_set_ids=[x.pk for x in others], related_list_ids=[x.pk for x in others]
        )

        instance.save()
        instance.refresh_from_db()

        self.assertEqual(instance.related_set_ids, [x.pk for x in others])
        self.assertEqual(instance.related_list_ids, [x.pk for x in others])

    def test_related_set_field_cross_database(self):
        a = ModelDatabaseA.objects.create()
        b = ModelDatabaseB.objects.create()

        self.assertItemsEqual(a.set_of_bs.all(), [])

        a.set_of_bs.add(b)
        a.save()
        self.assertItemsEqual(a.set_of_bs.all(), [b])

        a.set_of_bs.remove(b)
        a.save()
        self.assertItemsEqual(a.set_of_bs.all(), [])

    def test_related_list_field_cross_database(self):
        a = ModelDatabaseA.objects.create()
        b = ModelDatabaseB.objects.create()

        self.assertItemsEqual(a.list_of_bs.all(), [])

        a.list_of_bs.add(b)
        a.save()
        self.assertItemsEqual(a.list_of_bs.all(), [b])

        a.list_of_bs.remove(b)
        a.save()
        self.assertItemsEqual(a.list_of_bs.all(), [])


class RelatedListFieldModelTests(TestCase):
    def test_values_list_queries_work(self):
        a, b = ISOther.objects.create(name="A"), ISOther.objects.create(name="B")
        thing = ISModel.objects.create(related_list_ids=[a.pk, b.pk])

        result = list(thing.related_list.values_list("pk"))
        self.assertEqual(result, [(a.pk,), (b.pk,)])

        result = list(thing.related_list.values_list("pk", flat=True))
        self.assertEqual(result, [a.pk, b.pk])

        result = list(thing.related_list.values_list("name", flat=True))
        self.assertEqual(result, ["A", "B"])

        result = list(thing.related_list.values_list("name"))
        self.assertEqual(result, [("A",), ("B",)])

        result = list(thing.related_list.values_list("pk", "name"))
        self.assertEqual(result, [(a.pk, "A"), (b.pk, "B")])

    def test_indexing_doesnt_over_fetch(self):
        a, b = ISOther.objects.create(), ISOther.objects.create()
        thing = ISModel.objects.create(related_list_ids=[a.pk, b.pk])

        with sleuth.watch("gcloudc.db.backends.datastore.meta_queries.QueryByKeys.__init__") as get:
            thing.related_list.all()[0]

            self.assertEqual(1, get.call_count)
            self.assertEqual(1, len(get.calls[0].args[3]))

    def test_can_update_related_field_from_form(self):
        related = ISOther.objects.create()
        thing = ISModel.objects.create(related_list=[related])
        before_list = thing.related_list
        thing.related_list.field.save_form_data(thing, [])
        self.assertNotEqual(before_list.all(), thing.related_list.all())

    def test_filtering_on_iterable_fields(self):
        related1 = ISOther.objects.create()
        related2 = ISOther.objects.create()
        related3 = ISOther.objects.create()
        thing = ISModel.objects.create(related_list=[related1, related2])
        # filtering using __contains lookup with ListField:
        qry = ISModel.objects.filter(related_list__contains=related1)
        self.assertEqual(sorted(x.pk for x in qry), [thing.pk])
        # filtering using __overlap lookup with ListField:
        qry = ISModel.objects.filter(related_list__overlap=[related2, related3])
        self.assertEqual(sorted(x.pk for x in qry), [thing.pk])

    def test_saving_forms(self):
        class TestForm(forms.ModelForm):
            class Meta:
                model = ISModel
                fields = ("related_list",)

        related = ISOther.objects.create()
        post_data = {"related_list": [str(related.pk)]}

        form = TestForm(post_data)
        self.assertTrue(form.is_valid())
        instance = form.save()
        self.assertEqual([related.pk], instance.related_list_ids)

    def test_remove_duplicates(self):
        """
        make sure that remove_duplicates option works fine for RelatedListField
        """
        instance_one, instance_two, instance_three, instance_four, instance_five = [
            RelatedCharFieldModel.objects.create(char_field=str(x)) for x in range(5)
        ]
        data = {
            "related_list_field": [
                instance_two.pk,
                instance_three.pk,
                instance_one.pk,
                instance_two.pk,
                instance_four.pk,
            ]
        }
        form = RelatedListFieldRemoveDuplicatesForm(data)
        self.assertTrue(form.is_valid())
        obj = form.save()

        self.assertEqual(
            obj.related_list_field_ids, [instance_two.pk, instance_three.pk, instance_one.pk, instance_four.pk]
        )
        obj.related_list_field.add(instance_four, instance_five)
        obj.save()
        self.assertEqual(
            obj.related_list_field_ids,
            [instance_two.pk, instance_three.pk, instance_one.pk, instance_four.pk, instance_five.pk],
        )


class RelatedSetFieldModelTests(TestCase):
    def test_can_update_related_field_from_form(self):
        related = ISOther.objects.create()
        thing = ISModel.objects.create(related_things={related})
        before_set = thing.related_things
        thing.related_list.field.save_form_data(thing, set())
        thing.save()
        self.assertNotEqual(before_set.all(), thing.related_things.all())

    def test_prefetch_related(self):
        tag = Tag.objects.create(name="Apples")

        for i in range(2):
            Post.objects.create(content="Bananas", tags={tag})

        posts = list(Post.objects.prefetch_related("tags").all())
        self.assertNumQueries(0, list, posts[0].tags.all())

    def test_saving_forms(self):
        class TestForm(forms.ModelForm):
            class Meta:
                model = ISModel
                fields = ("related_things",)

        related = ISOther.objects.create()
        post_data = {"related_things": [str(related.pk)]}

        form = TestForm(post_data)
        self.assertTrue(form.is_valid())
        instance = form.save()
        self.assertEqual({related.pk}, instance.related_things_ids)


class InstanceListFieldTests(TestCase):
    def test_deserialization(self):
        i1 = ISOther.objects.create(pk=1)
        i2 = ISOther.objects.create(pk=2)
        # Does the to_python need to return ordered list? SetField test only passes because the set
        # happens to order it correctly
        self.assertItemsEqual([i1, i2], ISModel._meta.get_field("related_list").to_python("[1, 2]"))

    def test_prefetch_related(self):
        tags = [Tag.objects.create(name="1"), Tag.objects.create(name="2"), Tag.objects.create(name="3")]

        # Extra one to make sure we're filtering properly
        Tag.objects.create(name="unused")

        for i in range(3):
            Post.objects.create(content="Bananas", ordered_tags=tags)

        with self.assertNumQueries(2):
            # 1 query on Posts + 1 query on Tags
            posts = list(Post.objects.prefetch_related("ordered_tags").all())

        with self.assertNumQueries(0):
            posts[0].tags.all()
            posts[1].tags.all()
            posts[2].tags.all()

        self.assertEqual(posts[0].ordered_tags.all()[0].name, "1")
        self.assertEqual(posts[0].ordered_tags.all()[1].name, "2")
        self.assertEqual(posts[0].ordered_tags.all()[2].name, "3")

    def test_default_on_delete_does_nothing(self):
        child = ISOther.objects.create(pk=1)
        parent = ISModel.objects.create(related_list=[child])

        child.delete()

        try:
            parent = ISModel.objects.get(pk=parent.pk)
            self.assertEqual([1], parent.related_list_ids)
        except ISModel.DoesNotExist:
            self.fail("Parent instance was deleted, apparently by on_delete=CASCADE")

    def test_save_and_load_empty(self):
        """
        Create a main object with no related items,
        get a copy of it back from the db and try to read items.
        """
        main = ISModel.objects.create()
        main_from_db = ISModel.objects.get(pk=main.pk)

        # Fetch the container from the database and read its items
        self.assertItemsEqual(main_from_db.related_list.all(), [])

    def test_basic_usage(self):
        main = ISModel.objects.create()
        other = ISOther.objects.create(name="test")
        other2 = ISOther.objects.create(name="test2")

        main.related_list.add(other)
        main.save()

        self.assertEqual([other.pk], main.related_list_ids)
        self.assertEqual(list(ISOther.objects.filter(pk__in=main.related_list_ids)), list(main.related_list.all()))
        self.assertEqual([main], list(other.ismodel_list.all()))

        main.related_list.remove(other)
        self.assertFalse(main.related_list)

        main.related_list = [other2]
        self.assertEqual([other2.pk], main.related_list_ids)

        with self.assertRaises(AttributeError):
            other.ismodel_list = [main]

        without_reverse = RelationWithoutReverse.objects.create(name="test3")
        self.assertFalse(hasattr(without_reverse, "ismodel_list"))

    def test_add_to_empty(self):
        """
        Create a main object with no related items,
        get a copy of it back from the db and try to add items.
        """
        main = ISModel.objects.create()
        main_from_db = ISModel.objects.get(pk=main.pk)

        other = ISOther.objects.create()
        main_from_db.related_list.add(other)
        main_from_db.save()

    def test_add_another(self):
        """
        Create a main object with related items,
        get a copy of it back from the db and try to add more.
        """
        main = ISModel.objects.create()
        other1 = ISOther.objects.create()
        main.related_things.add(other1)
        main.save()

        main_from_db = ISModel.objects.get(pk=main.pk)
        other2 = ISOther.objects.create()

        main_from_db.related_list.add(other2)
        main_from_db.save()

    def test_multiple_objects(self):
        main = ISModel.objects.create()
        other1 = ISOther.objects.create()
        other2 = ISOther.objects.create()

        main.related_list.add(other1, other2)
        main.save()

        main_from_db = ISModel.objects.get(pk=main.pk)
        self.assertEqual(main_from_db.related_list.count(), 2)

    def test_deletion(self):
        """
        Delete one of the objects referred to by the related field
        """
        main = ISModel.objects.create()
        other = ISOther.objects.create()
        main.related_list.add(other)
        main.save()

        other.delete()
        self.assertEqual(main.related_list.count(), 0)

    def test_ordering_is_maintained(self):
        main = ISModel.objects.create()
        other = ISOther.objects.create()
        other1 = ISOther.objects.create()
        other2 = ISOther.objects.create()
        other3 = ISOther.objects.create()
        main.related_list.add(other, other1, other2, other3)
        main.save()
        self.assertEqual(main.related_list.count(), 4)
        self.assertEqual([other.pk, other1.pk, other2.pk, other3.pk], main.related_list_ids)
        self.assertItemsEqual([other, other1, other2, other3], main.related_list.all())
        main.related_list.clear()
        main.save()
        self.assertEqual([], main.related_list_ids)

    def test_duplicates_maintained(self):
        """
            For whatever reason you might want many of the same relation in the
            list
        """
        main = ISModel.objects.create()
        other = ISOther.objects.create()
        other1 = ISOther.objects.create()
        other2 = ISOther.objects.create()
        other3 = ISOther.objects.create()
        main.related_list.add(other, other1, other2, other1, other3)
        main.save()
        self.assertEqual([other.pk, other1.pk, other2.pk, other1.pk, other3.pk], main.related_list_ids)
        self.assertItemsEqual([other, other1, other2, other1, other3], main.related_list.all())

    def test_slicing(self):
        main = ISModel.objects.create()
        other = ISOther.objects.create()
        other1 = ISOther.objects.create()
        other2 = ISOther.objects.create()
        other3 = ISOther.objects.create()
        main.related_list.add(other, other1, other2, other1, other3)
        main.save()
        self.assertItemsEqual([other, other1], main.related_list.all()[:2])
        self.assertItemsEqual([other1], main.related_list.all()[1:2])
        self.assertEqual(other1, main.related_list.all()[1:2][0])

    def test_filtering(self):
        main = ISModel.objects.create()
        other = ISOther.objects.create(name="one")
        other1 = ISOther.objects.create(name="two")
        other2 = ISOther.objects.create(name="one")
        ISOther.objects.create(name="three")
        main.related_list.add(other, other1, other2, other1, other2)
        main.save()
        self.assertItemsEqual([other, other2, other2], main.related_list.filter(name="one"))

    def test_related_list_field_serializes_and_deserializes(self):
        obj = ISModel.objects.create()
        foo = ISOther.objects.create(name="foo")
        bar = ISOther.objects.create(name="bar")
        obj.related_list.add(foo, bar)
        obj.save()

        data = serializers.serialize("json", [obj])
        new_obj = next(serializers.deserialize("json", data)).object
        self.assertEqual(list(new_obj.related_list.all()), [foo, bar])


class InstanceSetFieldTests(TestCase):
    def test_deserialization(self):
        i1 = ISOther.objects.create(pk=1)
        i2 = ISOther.objects.create(pk=2)

        self.assertEqual(set([i1, i2]), ISModel._meta.get_field("related_things").to_python("[1, 2]"))

    def test_basic_usage(self):
        main = ISModel.objects.create()
        other = ISOther.objects.create(name="test")
        other2 = ISOther.objects.create(name="test2")

        main.related_things.add(other)
        main.save()

        self.assertEqual({other.pk}, main.related_things_ids)
        self.assertEqual(list(ISOther.objects.filter(pk__in=main.related_things_ids)), list(main.related_things.all()))

        self.assertItemsEqual([main], ISModel.objects.filter(related_things__contains=other).all())
        self.assertItemsEqual([main], list(other.ismodel_set.all()))

        main.related_things.remove(other)
        self.assertFalse(main.related_things_ids)

        main.related_things = {other2}
        self.assertEqual({other2.pk}, main.related_things_ids)

        with self.assertRaises(AttributeError):
            other.ismodel_set = {main}

        without_reverse = RelationWithoutReverse.objects.create(name="test3")
        self.assertFalse(hasattr(without_reverse, "ismodel_set"))

    def test_save_and_load_empty(self):
        """
        Create a main object with no related items,
        get a copy of it back from the db and try to read items.
        """
        main = ISModel.objects.create()
        main_from_db = ISModel.objects.get(pk=main.pk)

        # Fetch the container from the database and read its items
        self.assertItemsEqual(main_from_db.related_things.all(), [])

    def test_add_to_empty(self):
        """
        Create a main object with no related items,
        get a copy of it back from the db and try to add items.
        """
        main = ISModel.objects.create()
        main_from_db = ISModel.objects.get(pk=main.pk)

        other = ISOther.objects.create()
        main_from_db.related_things.add(other)
        main_from_db.save()

    def test_add_another(self):
        """
        Create a main object with related items,
        get a copy of it back from the db and try to add more.
        """
        main = ISModel.objects.create()
        other1 = ISOther.objects.create()
        main.related_things.add(other1)
        main.save()

        main_from_db = ISModel.objects.get(pk=main.pk)
        other2 = ISOther.objects.create()

        main_from_db.related_things.add(other2)
        main_from_db.save()

    def test_multiple_objects(self):
        main = ISModel.objects.create()
        other1 = ISOther.objects.create()
        other2 = ISOther.objects.create()

        main.related_things.add(other1, other2)
        main.save()

        main_from_db = ISModel.objects.get(pk=main.pk)
        self.assertEqual(main_from_db.related_things.count(), 2)

    def test_deletion(self):
        """
        Delete one of the objects referred to by the related field
        """
        main = ISModel.objects.create()
        other = ISOther.objects.create()
        main.related_things.add(other)
        main.save()

        other.delete()
        self.assertEqual(main.related_things.count(), 0)

    def test_querying_with_isnull(self):
        obj = ISModel.objects.create()

        self.assertItemsEqual([obj], ISModel.objects.filter(related_things__isempty=True))
        self.assertItemsEqual([obj], ISModel.objects.filter(related_things_ids__isempty=True))

    def test_related_set_field_serializes_and_deserializes(self):
        obj = ISModel.objects.create()
        foo = ISOther.objects.create(name="foo")
        bar = ISOther.objects.create(name="bar")
        obj.related_things.add(foo, bar)
        obj.save()

        data = serializers.serialize("json", [obj])

        new_obj = next(serializers.deserialize("json", data)).object
        self.assertEqual(set(new_obj.related_things.all()), set([foo, bar]))


class TestGenericRelationField(TestCase):
    def test_basic_usage(self):
        instance = GenericRelationModel.objects.create()
        self.assertIsNone(instance.relation_to_anything)

        thing = ISOther.objects.create()
        instance.relation_to_anything = thing
        instance.save()

        self.assertTrue(instance.relation_to_anything_id)

        instance = GenericRelationModel.objects.get()
        self.assertEqual(thing, instance.relation_to_anything)

    def test_overridden_dbtable(self):
        """ Check that the related object having a custom `db_table` doesn't affect the functionality. """
        instance = GenericRelationModel.objects.create()
        self.assertIsNone(instance.relation_to_anything)

        weird = RelationWithOverriddenDbTable.objects.create()
        instance.relation_to_anything = weird
        instance.save()

        self.assertTrue(instance.relation_to_anything)

        instance = GenericRelationModel.objects.get()
        self.assertEqual(weird, instance.relation_to_anything)

    def test_querying(self):
        thing = ISOther.objects.create()
        instance = GenericRelationModel.objects.create(relation_to_anything=thing)
        self.assertEqual(GenericRelationModel.objects.filter(relation_to_anything=thing)[0], instance)

    def test_unique(self):
        thing = ISOther.objects.create()
        instance = GenericRelationModel.objects.create(unique_relation_to_anything=thing)
        # Trying to create another instance which relates to the same 'thing' should fail
        self.assertRaises(IntegrityError, GenericRelationModel.objects.create, unique_relation_to_anything=thing)
        # But creating 2 objects which both have `unique_relation_to_anything` set to None should be fine
        instance.unique_relation_to_anything = None
        instance.save()
        GenericRelationModel.objects.create(unique_relation_to_anything=None)
        GenericRelationModel.objects.create()  # It should work even if we don't explicitly set it to None

    def test_saving_forms(self):
        class TestForm(forms.ModelForm):
            class Meta:
                model = GenericRelationModel
                fields = ("relation_to_anything",)

        related = ISOther.objects.create()
        post_data = {"relation_to_anything_0": related.__class__._meta.db_table, "relation_to_anything_1": related.pk}

        form = TestForm(post_data)
        self.assertTrue(form.is_valid())
        instance = form.save()
        self.assertEqual(related, instance.relation_to_anything)
