# STANDARD LIB
from abc import ABCMeta
import copy

# THIRD PARTY
from django import forms
from django.db import router, models
from django.db.models.query import QuerySet
from django.db.models.fields.related import ForeignObject, ForeignObjectRel
from django.utils.functional import cached_property
from django.core.exceptions import ImproperlyConfigured, ValidationError
from gcloudc.forms.fields import encode_pk, GenericRelationFormfield, OrderedModelMultipleChoiceField

from django.db.models.query import FlatValuesListIterable

# gcloudc
from gcloudc.core.validators import MinItemsValidator, MaxItemsValidator
from gcloudc.db.models.fields.iterable import IsEmptyLookup, ContainsLookup, OverlapLookup, _serialize_value


class RelatedIteratorRel(ForeignObjectRel):
    def __init__(self, field, to, related_name=None, limit_choices_to=None, on_delete=models.DO_NOTHING, **kwargs):
        self.field = field
        self.model = to
        self.related_name = related_name
        self.related_query_name = None
        self.field_name = None
        self.parent_link = None
        self.on_delete = on_delete
        self.symmetrical = False
        self.limit_choices_to = {} if limit_choices_to is None else limit_choices_to
        self.multiple = True

    def is_hidden(self):
        "Should the related object be hidden?"
        return self.related_name and self.related_name[-1] == "+"

    def set_field_name(self):
        self.field_name = self.field_name or self.model._meta.pk.name

    def get_related_field(self):
        """
        Returns the field in the to' object to which this relationship is tied
        (this is always the primary key on the target model). Provided for
        symmetry with ManyToOneRel.
        """
        return self.model._meta.pk


class OrderedQuerySet(QuerySet):
    def _fetch_all(self):
        """
            Fetch all uses the standard iterator but sorts the values on the
            way out, this maintains the lazy evaluation of querysets
        """

        def locate_pk_column(query):
            pk_name = self.model._meta.pk.name
            if "pk" in query.values_select:
                return query.values_select.index("pk")
            elif pk_name in query.values_select:
                return query.values_select.index(pk_name)

        if self._result_cache is None:
            # Making this work efficiently is tricky. We never want to fetch more than self.ordered_pks
            # and the __getitem__ implementation sets this to a single item, so in that case we only want
            # to fetch one. So we do a few things here:

            # 1. We clone the Queryset as a normal Queryset, then we do a pk__in on the ordered_pks.
            # 2. We add the primary key if this was a values_list query without it being specified, otherwise
            #    we can't match up the ordering
            # 3. We execute the clone, and then remove the additional PK column from the result set

            # There are various combinations to handle depending on whether it's a "flat" values_list or
            # whether or not we added the PK manually to the result set

            clone = QuerySet(model=self.model, query=self.query, using=self._db)
            clone._iterable_class = self._iterable_class
            clone = clone.filter(pk__in=self.ordered_pks)

            pk_col = 0
            pk_added = False

            values_select = clone.query.values_select

            if values_select:
                pk_col = locate_pk_column(clone.query)
                if pk_col is None:
                    # Manually add the PK to the result set
                    clone = clone.values_list(*(["pk"] + list(values_select)))
                    values_select = [x.field.name for x in clone.query.select]
                    pk_col = 0
                    pk_added = True

            # Hit the database
            results = list(clone)

            ordered_results = []
            pk_hash = {}

            flat = self._iterable_class == FlatValuesListIterable

            for x in results:
                if isinstance(x, models.Model):
                    # standard query case
                    pk_hash[x.pk] = x
                elif len(values_select) == 1:
                    # Only PK case
                    pk_hash[x if flat else x[pk_col]] = x
                else:
                    # Multiple columns (either passed in, or as a result of the PK being added)
                    if flat:
                        pk_hash[x[pk_col]] = x[1] if pk_added else x
                    else:
                        pk_hash[x[pk_col]] = x[1:] if pk_added else x

            for pk in self.ordered_pks:
                obj = pk_hash.get(pk)
                if obj:
                    ordered_results.append(obj)
            self._result_cache = ordered_results
        if self._prefetch_related_lookups and not self._prefetch_done:
            self._prefetch_related_objects()

    def _clone(self, *args, **kwargs):
        """
            We need to attach the ordered_pk list on the clone to it continues
            through the chain
        """
        c = super(OrderedQuerySet, self)._clone(*args, **kwargs)
        c.ordered_pks = self.ordered_pks[:]
        return c

    def __getitem__(self, k):
        """
            Ok to get query slicing working properly we can't allow it
            to set limits on the internal query, because that is not the
            behavior we want, instead we slice the internal ordered_pk list
        """
        if not isinstance(k, (slice,) + (int,)):
            raise TypeError

        assert (not isinstance(k, slice) and (k >= 0)) or (
            isinstance(k, slice) and (k.start is None or k.start >= 0) and (k.stop is None or k.stop >= 0)
        )

        if self._result_cache is not None:
            return self._result_cache[k]

        if isinstance(k, slice):
            qs = self._clone()
            if k.start is not None:
                start = int(k.start)
            else:
                start = None
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = None
            qs.ordered_pks = qs.ordered_pks[start:stop]
            return list(qs)[:: k.step] if k.step else qs

        qs = self._clone()
        qs.ordered_pks = [qs.ordered_pks[k]]
        return list(qs)[0]


class RelatedIteratorManagerBase(object):
    def __init__(self, model, field, instance, reverse):
        super(RelatedIteratorManagerBase, self).__init__()
        self.model = model
        self.instance = instance
        self.field = field
        self.reverse = reverse
        self.ordered = field.retain_underlying_order

        if reverse:
            self.core_filters = {"%s__contains" % self.field.column: instance.pk}
        else:
            self.core_filters = {"pk__in": field.value_from_object(instance)}

    def get_prefetch_queryset(self, instances, queryset=None):
        related_model = self.field.related_model if hasattr(self.field, "related_model") else self.field.related.to
        if not queryset:
            queryset = related_model.objects.all()

        matchers = {}
        related_ids = set()
        for instance in instances:
            for related_id in getattr(instance, self.field.attname):
                related_ids.add(related_id)
                matchers.setdefault(related_id, []).append(instance.pk)

        queryset = queryset.filter(pk__in=related_ids)

        def duplicator(queryset):
            """
                We have to duplicate the related objects for each source instance that
                was passed in so we can convince Django to do the right thing when merging
            """
            for related_thing in queryset:
                for i, matcher in enumerate(matchers[related_thing.pk]):
                    if i == 0:
                        # Optimisation to prevent unnecessary extra copy
                        ret = related_thing
                    else:
                        ret = copy.copy(related_thing)

                    # Set an id so we can fetch things out in the lambdas below
                    ret._prefetch_instance_id = matcher
                    yield ret

        return (
            duplicator(queryset),
            lambda inst: inst._prefetch_instance_id,
            lambda obj: obj.pk,
            False,
            self.field.name,  # Use the field name as the cache name
            True
        )

    def get_queryset(self):
        db = self._db or router.db_for_read(self.model, instance=self.instance)

        if (
            hasattr(self.instance, "_prefetched_objects_cache")
            and self.field.name in self.instance._prefetched_objects_cache
        ):

            qs = self.instance._prefetched_objects_cache[self.field.name]

            if self.ordered:
                lookup = {}
                for item in qs._result_cache:
                    lookup[item.pk] = item

                # If this is a ListField make sure the result set retains the right order
                qs._result_cache = []
                for pk in getattr(self.instance, self.field.attname):
                    qs._result_cache.append(lookup[pk])
                return qs
            else:
                return qs
        elif self.ordered and not self.reverse:
            values = self.field.value_from_object(self.instance)
            qcls = OrderedQuerySet(self.model, using=db)
            qcls.ordered_pks = values[:]
        else:
            qcls = super(RelatedIteratorManagerBase, self).get_queryset()
        return qcls.using(db)._next_is_sticky().filter(**self.core_filters)

    def add(self, *values):
        for value in values:
            if not isinstance(value, self.model):
                raise TypeError("'%s' instance expected, got %r" % (self.model._meta.object_name, value))

            if not value.pk:
                raise ValueError("Model instances must be saved before they can be added to a related set")

            field_value = self.field.value_from_object(self.instance)
            # Depending on the type of iterable, but we want to maintain the
            # related .add() behavior
            if isinstance(field_value, list):
                field_value.append(value.pk)
            elif isinstance(field_value, set):
                field_value.add(value.pk)

    def remove(self, value):
        field_value = self.field.value_from_object(self.instance)
        # Depending on the type of iterable, but we want to maintain the
        # related .remove() behavior
        if isinstance(field_value, list):
            field_value.remove(value.pk)
        elif isinstance(field_value, set):
            field_value.discard(value.pk)

    def clear(self):
        setattr(self.instance, self.field.attname, self.field.default())

    def __len__(self):
        return len(self.field.value_from_object(self.instance))


def create_related_iter_manager(superclass, rel):
    """ Create a manager for the (reverse) relation which subclasses the related model's default manager. """

    class RelatedIteratorManager(RelatedIteratorManagerBase, superclass):
        pass

    return RelatedIteratorManager


class RelatedIteratorObjectsDescriptor(object):
    # This class provides the functionality that makes the related-object
    # managers available as attributes on a model class, for fields that have
    # multiple "remote" values and have a ManyToManyField pointed at them by
    # some other model (rather than having a ManyToManyField themselves).
    # In the example "publication.article_set", the article_set attribute is a
    # ManyRelatedObjectsDescriptor instance.
    def __init__(self, related):
        self.related = related  # RelatedObject instance

    @cached_property
    def related_manager_cls(self):
        """
            Dynamically create a class that subclasses the related
            model's default manager.
        """

        manager = self.related.related_model._default_manager.__class__

        return create_related_iter_manager(manager, self.related.field.remote_field)

    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self

        rel_model = self.related.related_model
        rel_field = self.related.field

        manager = self.related_manager_cls(model=rel_model, field=rel_field, instance=instance, reverse=True)

        return manager

    def __set__(self, obj, value):
        raise AttributeError("You can't set the reverse relation directly")


class ReverseRelatedObjectsDescriptor(object):
    # This class provides the functionality that makes the related-object
    # managers available as attributes on a model class, for fields that have
    # multiple "remote" values and have a ManyToManyField defined in their
    # model (rather than having another model pointed *at* them).
    # In the example "article.publications", the publications attribute is a
    # ReverseManyRelatedObjectsDescriptor instance.
    def __init__(self, m2m_field):
        self.field = m2m_field

    @cached_property
    def related_manager_cls(self):
        # Dynamically create a class that subclasses the related model's
        # default manager.
        return create_related_iter_manager(
            self.field.remote_field.model._default_manager.__class__, self.field.remote_field.model
        )

    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self

        manager = self.related_manager_cls(
            model=self.field.remote_field.model, field=self.field, instance=instance, reverse=False
        )

        return manager

    def __set__(self, obj, value):
        obj.__dict__[self.field.attname] = self.field.to_python([x.pk for x in value])


class RelatedContainsLookup(ContainsLookup):
    def get_prep_lookup(self):
        # If this is an instance, then we want to lookup based on the PK
        if isinstance(self.rhs, models.Model):
            self.rhs = self.rhs.pk
        return super(RelatedContainsLookup, self).get_prep_lookup()


class RelatedOverlapLookup(OverlapLookup):
    def get_prep_lookup(self):
        # Transform any instances to primary keys
        self.rhs = [x.pk if isinstance(x, models.Model) else x for x in self.rhs]
        return super(RelatedOverlapLookup, self).get_prep_lookup()


class RelatedIteratorField(ForeignObject):

    __metaclass__ = ABCMeta

    requires_unique_target = False
    generate_reverse_relation = True
    empty_strings_allowed = False
    retain_underlying_order = False

    rel_class = RelatedIteratorRel

    def __init__(self, to, limit_choices_to=None, related_name=None, on_delete=models.DO_NOTHING, **kwargs):
        # Make sure that we do nothing on cascade by default
        self._check_sane_on_delete_value(on_delete)

        from_fields = ["self"]
        to_fields = [None]

        min_length, max_length = self._sanitize_min_and_max_length(kwargs)

        super(RelatedIteratorField, self).__init__(
            to,
            on_delete,
            from_fields,
            to_fields,
            related_name=related_name,
            limit_choices_to=limit_choices_to,
            **kwargs
        )

        # Now that self.validators has been set up, we can add the min/max legnth validators
        if min_length is not None:
            self.validators.append(MinItemsValidator(min_length))
        if max_length is not None:
            self.validators.append(MaxItemsValidator(max_length))

    def _check_sane_on_delete_value(self, on_delete):
        if on_delete == models.CASCADE:
            raise ImproperlyConfigured(
                "on_delete=CASCADE is disabled for iterable fields as this will "
                "wipe out the instance when the field still has related objects"
            )

        if on_delete in (models.SET_NULL, models.SET_DEFAULT):
            raise ImproperlyConfigured(
                "Using an on_delete value of {} will cause undesirable behavior"
                " (e.g. wipeout the entire list) if you really want to do that "
                "then use models.SET instead and return an empty list/set"
            )

    def _sanitize_min_and_max_length(self, kwargs):
        # Pop the 'min_length' and 'max_length' from the kwargs, if they're there, as this avoids
        # 'min_length' causing an error when calling super()
        min_length = kwargs.pop("min_length", None)
        max_length = kwargs.pop("max_length", None)

        # Check that if there's a min_length that blank is not True.  This is partly because it
        # doesn't make sense, and partly because if the value (i.e. the list or set) is empty then
        # Django will skip the validators, thereby skipping the min_length check.
        if min_length and kwargs.get("blank"):
            raise ImproperlyConfigured("Setting blank=True and min_length=%d is contradictory." % min_length)

        return min_length, max_length

    def _get_lookup_constraint(self, constraint_class, alias, targets, sources, lookups, raw_value):
        from django.core import exceptions
        from django.db.models.sql.where import AND

        root_constraint = constraint_class()

        # we need some checks from ForeignObject.get_lookup_constraint
        # before we could check the lookups.
        assert len(targets) == len(sources)
        if len(lookups) > 1:
            raise exceptions.FieldError("%s does not support nested lookups" % self.__class__.__name__)

        lookup_type = lookups[0]
        target = targets[0]
        source = sources[0]

        # use custom Lookups when applicable
        if lookup_type in ["isempty", "overlap", "contains"]:
            if lookup_type == "isempty":
                root_constraint.add(IsEmptyLookup(target.get_col(alias, source), raw_value), AND)
            elif lookup_type == "overlap":
                root_constraint.add(RelatedOverlapLookup(target.get_col(alias, source), raw_value), AND)
            elif lookup_type == "contains":
                root_constraint.add(RelatedContainsLookup(target.get_col(alias, source), raw_value), AND)
            return root_constraint
        elif lookup_type in ("in", "exact", "isnull"):
            raise TypeError(
                "%s doesn't allow exact, in or isnull. Use contains, overlap or isempty respectively"
                % self.__class__.__name__
            )

        return super(RelatedIteratorField, self).get_lookup_constraint(
            constraint_class, alias, targets, sources, lookups, raw_value
        )

    def deconstruct(self):
        name, path, args, kwargs = super(RelatedIteratorField, self).deconstruct()
        # We hardcode a number of arguments for RelatedIteratorField, those arguments need to be removed here
        for hardcoded_kwarg in ["to_fields", "from_fields", "on_delete"]:
            del kwargs[hardcoded_kwarg]

        return name, path, args, kwargs

    def get_attname(self):
        return "%s_ids" % self.name

    def get_attname_column(self):
        attname = self.get_attname()
        column = self.db_column or attname
        return attname, column

    def contribute_to_class(self, cls, name):
        # To support multiple relations to self, it's useful to have a non-None
        # related name on symmetrical relations for internal reasons. The
        # concept doesn't make a lot of sense externally ("you want me to
        # specify *what* on my non-reversible relation?!"), so we set it up
        # automatically. The funky name reduces the chance of an accidental
        # clash.
        rel_to = self.remote_field.model if self.remote_field else None
        if rel_to == "self" or rel_to == cls._meta.object_name:
            self.remote_field.related_name = "%s_rel_+" % name

        super(RelatedIteratorField, self).contribute_to_class(cls, name)

        # Add the descriptor for the m2m relation
        setattr(cls, self.name, ReverseRelatedObjectsDescriptor(self))

    def contribute_to_related_class(self, cls, related):
        # Internal M2Ms (i.e., those with a related name ending with '+')
        # and swapped models don't get a related descriptor.
        if not self.remote_field.is_hidden() and not related.model._meta.swapped:
            setattr(cls, related.get_accessor_name(), RelatedIteratorObjectsDescriptor(related))

    def get_db_prep_save(self, *args, **kwargs):
        ret = super(RelatedIteratorField, self).get_db_prep_save(*args, **kwargs)

        if not ret:
            return None

        if isinstance(ret, set):
            ret = list(ret)

        ret = [self.remote_field.model._meta.pk.clean(x, self.remote_field.model) for x in ret]

        return ret

    def get_db_prep_lookup(self, *args, **kwargs):
        ret = super(RelatedIteratorField, self).get_db_prep_lookup(*args, **kwargs)

        if not ret:
            return []

        if isinstance(ret, set):
            ret = list(ret)
        return ret

    def value_to_string(self, obj):
        return u"[" + ",".join(_serialize_value(o) for o in self.value_from_object(obj)) + "]"

    def formfield(self, **kwargs):
        db = kwargs.pop("using", None)
        form_class = kwargs.pop("form_class", forms.ModelMultipleChoiceField)
        defaults = {
            "form_class": form_class,
            "queryset": self.remote_field.model._default_manager.using(db).complex_filter(
                self.remote_field.limit_choices_to
            ),
        }
        defaults.update(kwargs)
        # If initial is passed in, it's a list of related objects, but the
        # MultipleChoiceField takes a list of IDs.
        if defaults.get("initial") is not None:
            initial = defaults["initial"]
            if callable(initial):
                initial = initial()
            defaults["initial"] = [i._get_pk_val() for i in initial]
        return super(RelatedIteratorField, self).formfield(**defaults)

    def get_lookup(self, lookup_name):
        if lookup_name == "isempty":
            return IsEmptyLookup
        elif lookup_name == "overlap":
            return RelatedOverlapLookup
        elif lookup_name == "contains":
            return RelatedContainsLookup
        elif lookup_name in ("in", "exact", "isnull"):
            raise TypeError("RelatedIteratorFields don't allow exact, in or isnull. Use contains, overlap or isempty")

        return super(RelatedIteratorField, self).get_lookup(lookup_name)

    def to_python(self, value):
        if value is None:
            return list()

        # Deal with deserialization from a string
        if isinstance(value, str):
            if not (value.startswith("[") and value.endswith("]")):
                raise ValidationError("Invalid input for {} instance", type(self).__name__)

            value = value[1:-1].strip()

            if not value:
                return list()

            ids = [
                self.remote_field.model._meta.pk.to_python(x.strip("'").strip('"'))
                for x in value.split(",")
                if len(value) > 2
            ]
            # Annoyingly Django special cases FK and M2M in the Python deserialization code,
            # to assign to the attname, whereas all other fields (including this one) are required to
            # populate field.name instead. So we have to query here... we have no choice :(
            objs = self.remote_field.model._default_manager.db_manager("default").in_bulk(ids)

            # retain order
            return [objs.get(_id) for _id in ids if _id in objs]

        return list(value)


RelatedIteratorField.register_lookup(RelatedContainsLookup)
RelatedIteratorField.register_lookup(RelatedOverlapLookup)
RelatedIteratorField.register_lookup(IsEmptyLookup)


class RelatedSetField(RelatedIteratorField):
    def db_type(self, connection):
        return "set"

    def __init__(self, *args, **kwargs):

        kwargs["default"] = set
        kwargs["null"] = True

        super(RelatedSetField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(RelatedSetField, self).deconstruct()
        # We hardcode a number of arguments for RelatedIteratorField, those arguments need to be removed here
        for hardcoded_kwarg in ["default", "null"]:
            del kwargs[hardcoded_kwarg]

        return name, path, args, kwargs

    def to_python(self, value):
        return set(super(RelatedSetField, self).to_python(value))

    def save_form_data(self, instance, data):
        setattr(instance, self.attname, set())  # Wipe out existing things
        for value in data:
            # If this is a model instance then add the pk
            if isinstance(value, self.remote_field.model):
                value = value.pk

            field = getattr(instance, self.attname)
            field.add(value)


class RelatedListField(RelatedIteratorField):
    retain_underlying_order = True

    def db_type(self, connection):
        return "list"

    def __init__(self, *args, **kwargs):

        kwargs["default"] = list
        kwargs["null"] = True
        self.remove_duplicates = kwargs.pop("remove_duplicates", False)

        super(RelatedListField, self).__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super(RelatedListField, self).deconstruct()
        # We hardcode a number of arguments for RelatedIteratorField, those arguments need to be removed here
        for hardcoded_kwarg in ["default", "null"]:
            del kwargs[hardcoded_kwarg]

        kwargs["remove_duplicates"] = self.remove_duplicates

        return name, path, args, kwargs

    def formfield(self, **kwargs):
        """
        Specify a OrderedModelMultipleChoiceField for the `form_class` so we
        can retain ordering.
        """
        # change the form_class in defaults from using ModelMultipleChoiceField
        # in preference for the gcloudc subclass OrderedModelMultipleChoiceField
        kwargs["form_class"] = kwargs.pop("form_class", OrderedModelMultipleChoiceField)
        return super(RelatedListField, self).formfield(**kwargs)

    def save_form_data(self, instance, data):
        setattr(instance, self.attname, [])  # Wipe out existing things
        for value in data:
            # If this is a model instance then grab the PK
            if isinstance(value, self.remote_field.model):
                value = value.pk

            # Add to the underlying list
            field = getattr(instance, self.attname)
            field.append(value)

    def pre_save(self, model_instance, add):
        value = super(RelatedListField, self).pre_save(model_instance, add)

        if value and self.remove_duplicates:
            seen = set()
            value = [x for x in value if not (x in seen or seen.add(x))]
            # We should also update the model attribute to hold correct reference
            setattr(model_instance, self.attname, value)

        return value


class GRCreator(property):

    """
    This is like django.db.models.fields.subclassing.Creator, but does its
    magic on get instead of set.
    """

    def __init__(self, field):
        self.field = field
        self.attname = field.get_attname()
        super(GRCreator, self).__init__(self.get, self.set)

    def get(self, obj):
        cache_attr = "_{}_cache".format(self.attname)
        if getattr(obj, cache_attr, None) is None:
            setattr(obj, cache_attr, self.field.__get__(obj))
        return getattr(obj, cache_attr)

    def set(self, obj, value):
        obj.__dict__[self.attname] = self.field.get_prep_value(value)
        if isinstance(value, models.Model):
            setattr(obj, "_{}_cache".format(self.attname), value)


class GRReverseCreator(property):

    """Prevent forms setting whatever_id to an object"""

    def __init__(self, field):
        self.field = field
        self.attname = field.get_attname()
        super(GRReverseCreator, self).__init__(self.get, self.set)

    def get(self, obj):
        return obj.__dict__[self.attname]

    def set(self, obj, value):
        if not isinstance(value, str):
            value = self.field.get_prep_value(value)
        obj.__dict__[self.attname] = value


class GenericRelationField(models.Field):
    """ Similar to django.contrib.contenttypes.generic.GenericForeignKey, but
        doesn't require all of the contenttypes cruft.

        Empty NULL values are specifically encoded so they can also be unique, this is different
        from Django's FK behaviour.
    """

    description = "Generic one-way relation field"
    form_class = GenericRelationFormfield

    def __init__(self, *args, **kwargs):
        kwargs.update(max_length=255)

        if "db_index" not in kwargs:
            kwargs["db_index"] = True

        super(GenericRelationField, self).__init__(*args, **kwargs)

    def contribute_to_class(self, cls, name):
        super(GenericRelationField, self).contribute_to_class(cls, name)
        setattr(cls, self.name, GRCreator(self))
        setattr(cls, self.attname, GRReverseCreator(self))

    def get_attname(self):
        return "{}_id".format(self.name)

    def set_attributes_from_name(self, name):
        super(GenericRelationField, self).set_attributes_from_name(name)

    @classmethod
    def encode_pk(cls, pk, obj_cls):
        return encode_pk(pk, obj_cls)

    def get_internal_type(self):
        return "CharField"

    def get_prep_value(cls, value):
        return cls.form_class.to_string(value)

    def __get__(self, instance):
        if instance is None:
            return self

        value = getattr(instance, self.attname)

        from gcloudc.forms.fields import decode_pk
        from gcloudc.db.backends.datastore.utils import get_model_from_db_table

        if value is None:
            return None

        model_ref, pk = decode_pk(value)
        try:
            return get_model_from_db_table(model_ref).objects.get(pk=pk)
        except AttributeError:
            raise ImproperlyConfigured("Unable to find model with db_table: {}".format(model_ref))

    def formfield(self, **kwargs):
        defaults = {"form_class": self.form_class}
        defaults.update(kwargs)
        return super(GenericRelationField, self).formfield(**defaults)

    def get_validator_unique_lookup_type(self):
        raise NotImplementedError()
