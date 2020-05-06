# STANDARD LIBRARY
import logging

# 3RD PARTY
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

# DJANGAE
from gcloudc.db.backends.datastore.dbapi import CouldBeSupportedError

logger = logging.getLogger("djangae")


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    """
    Work in progress!
    """

    def execute(self, sql, params=[]):
        """ Rather than executing SQL, we can probably (hopefully) do something cunning here
            involving deferring tasks to go and perform operations on the DB, probably using
            defer_iteration, or some kind of extension of that.

            The key will be knowing when the tasks have finished running.

            When running locally, there won't be a server running or a task queue stub set up
            (assuming we're doing mangage.py migrate) and so we will want to patch deferred.defer
            so that instead of being deferred to tasks, the functions are called immediately.
        """
        raise NotImplementedError("See docstring")

    def column_sql(self, model, field, include_default=False):
        """
        Takes a field and returns its column definition.
        The field must already have had set_attributes_from_name called.
        """
        # Erm... is this part of the external interface that we have to implement?  Hopefully not.

    def quote_value(self, value):
        """ SQL, lolz. """
        return value

    def create_model(self, model):
        """ Responsible for creating the table, indexes and unique constraints.
            I'm pretty sure that we can skip all of this for the Datastore.
        """
        pass

    def delete_model(self, model):
        """ Deletes a model from the database. """
        # The plan:
        # 1. Delete unique markers
        # 2. Delete entities
        # 3. Profit.
        raise NotImplementedError("See comments.")

    def alter_unique_together(self, model, old_unique_together, new_unique_together):
        """ Deals with a model changing its unique_together. """
        # The plan:
        # 1. Delete the old unique markers
        # 2. Create the new unique markers
        # Hopefully we can use djangae.contrib.uniquetool for this?
        raise NotImplementedError("See comments.")

    def alter_index_together(self, model, old_index_together, new_index_together):
        """ Deals with a model changing its index_together.
            Irrelevant for the Datastore.
        """
        pass

    def alter_db_table(self, model, old_db_table, new_db_table):
        """ Renames the table a model points to. """
        # Ideally this would be done as 3 separate steps:
        # (1) copy entities to new Datastore Kind
        # (2) deploy code which references new "table" (Kind)
        # (3) delete old entities
        # But Django's `manage.py migrate` approach isn't going to let us do that.
        # So we have 2 options:
        # 1. Implement it as a single mapreduce thing which *moves* the entities in a single
        # operation, thereby forcing your site to be offline/broken for a while.
        # 2. Not implement it and raise AreYouOffYourRockerError.
        raise NotImplementedError("See comments.")

    def alter_db_tablespace(self, model, old_db_tablespace, new_db_tablespace):
        """ Moves a model's table between tablespaces. """
        # Say what?
        # I guess this is equivalent to the Datastore namespaces.
        raise CouldBeSupportedError()

    def add_field(self, model, field):
        """ Creates a field on a model. """
        # Althought the Datastore is schemaless, when adding a new field we still want to deal with:
        # 1. If the default is not None, populate existing entities with default value.
        # 2. If the default is None, populate existing entities with None so that they are indexed.

        # Basically, there's no need to do any of this, *unless* you want to filter on this field.
        # But we don't know whether the project needs to filter on the field or not, so we have 2 options:
        # 1. Just run the populate tasks anyway.
        # 2. Find somewhere to add some kind of yes_please_populate_existing_entities option... ?
        # 3. Don't do anything, and allow the project to just specify the population task manually
        #    using the RunPython or RunDatastoreThing functionality that we will provide.

        # @adamalton votes for #3!

        # Oh, and did I mention that we may need to add unique markers if the field is unique?
        raise NotImplementedError

    def remove_field(self, model, field):
        """ Removes a field from a model. """
        # We need to:
        # 1. Delete any unique markers if the field was unique.
        # 2. Delete the data for the field.  Here we have several options:
        #     a. Not bother because deleting it will be almost as expensive as storing it (short term).
        #     b. Delete it.
        #     c. Allow the project to use our RunDatastoreThing functionality to delete it if they want to.
        raise NotImplementedError

    def alter_field(self, model, old_field, new_field, strict=False):
        """ Allows a field's type, uniqueness, nullability, default, column, constraints etc. to be modified. """
        # Nullability: doesn't matter.
        # Uniqueness: yes, we'll need to add/remove unique markers.
        # Default: doesn't matter.
        # Column: Erm... see `alter_db_table`.
        raise NotImplementedError
