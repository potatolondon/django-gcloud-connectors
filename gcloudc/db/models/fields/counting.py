import random
import warnings

from django.core.exceptions import ImproperlyConfigured
from django.db import models

from gcloudc.db.backends.datastore.indexing import transaction
from gcloudc.db.models.fields.related import (
    RelatedSetField,
    RelatedIteratorManagerBase,
    ReverseRelatedObjectsDescriptor,
)


MAX_ENTITIES_PER_GET = 1000
MAX_EG_PER_TXN = 25

# If the number of shards plus 1 (for the object to which they belong) is <= the max entity groups per
# transaction then we can do the populate() operation in a single transaction, which is nice, hence:
DEFAULT_SHARD_COUNT = MAX_SHARDS_PER_TRANSACTION = MAX_EG_PER_TXN - 1


class RelatedShardManager(RelatedIteratorManagerBase, models.Manager):
    """ This is what is given to you when you access the field attribute on an instance.  It's a
        model manager with the usual queryset methods (the same as for RelatedSetField) but with
        the additional increment()/decrement()/reset() methods for the counting.
    """

    def count(self):
        warnings.warn(
            "RelatedShardManager.count is deprecated. Use 'shard_count()' for the "
            "number of shards or 'value()' for the total counter value.",
            DeprecationWarning, 2
        )
        return super(RelatedShardManager, self).count()

    def increment(self, step=1):
        if step < 0:
            raise ValueError("Tried to increment with a negative number, use decrement instead")

        self._update_or_create_shard(step)

    def decrement(self, step=1):
        if step < 0:
            raise ValueError("Tried to decrement with a negative number, use increment instead")
        self._update_or_create_shard(-step)

    def value(self):
        """ Calcuate the aggregated sum of all the shard values. """
        shards = self.all().values_list('count', flat=True)
        return sum(shards)

    def shard_count(self):
        """ Return the number of shard objects. """
        return super(RelatedShardManager, self).count()

    def reset(self):
        """ Reset the counter to 0. """
        # This is not transactional because (1) that wouldn't work with > 24 shards, and (2) if
        # there are other threads doing increments/decrements at the same time then it doesn't make
        # any difference if they happen before or after our increment/decrement anyway.
        value = -self.value()
        self._update_or_create_shard(value, is_reset=True)

    def clear(self):
        # Override the default `clear` method of the parent class, as that only clears the list of
        # PKs but doesn't delete the related objects.  We want to delete the objects (shards) as well.
        self.reset()

    def populate(self):
        """ Create all the CounterShard objects which will be used by this field. Useful to prevent
            additional saves being caused when you call increment() or decrement() due to having to
            update the list of shard PKs on the instance.
        """
        total_to_create = self.field.shard_count - len(self)
        while total_to_create:
            with transaction.atomic(xg=True):
                # We must re-fetch the instance to ensure that we do this atomically, but we must
                # also update self.instance so that the calling code which is referencing
                # self.instance also gets the updated list of shard PKs
                new_instance = self.instance.__class__._default_manager.get(pk=self.instance.pk)
                new_instance_shard_pks = getattr(new_instance, self.field.attname, set())
                # Re-check / update the number to create based on the refreshed instance from the DB
                total_to_create = self.field.shard_count - len(new_instance_shard_pks)
                num_to_create = min(total_to_create, MAX_SHARDS_PER_TRANSACTION)

                new_shard_pks = set()
                for x in range(num_to_create):
                    new_shard_pks.add(self._create_shard(count=0).pk)

                new_instance_shard_pks.update(new_shard_pks)
                setattr(self.instance, self.field.attname, new_instance_shard_pks)
                models.Model.save(new_instance) # avoid custom save method, which might do DB lookups
                total_to_create -= num_to_create

    def _update_or_create_shard(self, step, is_reset=False):
        """ Find or create a random shard and alter its `count` by the given step. """
        shard_index = random.randint(0, self.field.shard_count - 1)
        # Converting the set to a list introduces some randomness in the ordering, but that's fine
        shard_pks = list(self.field.value_from_object(self.instance)) # needs to be indexable
        try:
            shard_pk = shard_pks[shard_index]
        except IndexError:
            # We don't have this many shards yet, so create a new one
            with transaction.atomic(xg=True):
                # We must re-fetch the instance to ensure that we do this atomically, but we must
                # also update self.instance so that the calling code which is referencing
                # self.instance also gets the updated list of shard PKs
                new_shard = self._create_shard(count=step)
                new_instance = self.instance.__class__._default_manager.get(pk=self.instance.pk)
                new_instance_shard_pks = getattr(new_instance, self.field.attname, set())
                new_instance_shard_pks.add(new_shard.pk)
                setattr(self.instance, self.field.attname, new_instance_shard_pks)
                models.Model.save(new_instance) # avoid custom save method, which might do DB lookups
                shard = new_shard
        else:
            with transaction.atomic():
                from djangae.models import CounterShard
                shard = CounterShard.objects.get(pk=shard_pk)
                shard.count += step
                shard.save()

        # if the ShardedCounter has on_change callback, run it now
        if self.field.on_change:
            self.field.on_change(self.instance, step, is_reset=is_reset)

    def _create_shard(self, count):
        from djangae.models import CounterShard
        return CounterShard.objects.create(
            count=count, label="%s.%s" % (self.instance._meta.db_table, self.field.name)
        )


class ReverseRelatedShardsDescriptor(ReverseRelatedObjectsDescriptor):
    """ Subclass of the RelatedSetField's ReverseRelatedObjectsDescriptor which overrides the
        related manager class and prevents setting of the field value directly.
    """
    @property
    def related_manager_cls(self):
        return RelatedShardManager

    def __set__(self, instance, value):
        raise AttributeError(
            "You should not try to set the value of a ShardedCounterField manually. "
            "Use the increment() or decrement() methods."
        )


class ShardedCounterField(RelatedSetField):

    def __init__(self, shard_count=DEFAULT_SHARD_COUNT, **kwargs):
        # Note that by removing the related_name by default we avoid reverse name clashes caused by
        # having multiple ShardedCounterFields on the same model.

        if "default" in kwargs:
            # We don't allow default because it causes all kinds of issues
            raise ImproperlyConfigured(
                "It is not possible to set a default value "
                "on a ShardedCounterField, use increment() after creation instead"
            )

        if "to" in kwargs:
            del kwargs["to"]

        self.shard_count = shard_count
        if shard_count > MAX_ENTITIES_PER_GET:
            raise ImproperlyConfigured(
                "ShardedCounterField.shard_count cannot be more than the Datastore is capable of "
                "fetching in a single Get operation (%d)" % MAX_ENTITIES_PER_GET
            )

        self.on_change = kwargs.pop("on_change", None)

        kwargs.setdefault("related_name", "+")
        super(ShardedCounterField, self).__init__('djangae.CounterShard', **kwargs)

    def contribute_to_class(self, cls, name):
        super(ShardedCounterField, self).contribute_to_class(cls, name)
        setattr(cls, self.name, ReverseRelatedShardsDescriptor(self))

    def deconstruct(self):
        name, path, args, kwargs = super(ShardedCounterField, self).deconstruct()

        del kwargs["to"]

        # Add the shard count if necessary
        if self.shard_count != DEFAULT_SHARD_COUNT:
            kwargs["shard_count"] = self.shard_count

        # Default is not a valid kwarg
        if "default" in kwargs:
            del kwargs["default"]

        return name, path, args, kwargs
