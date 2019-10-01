import copy
import threading
from functools import cmp_to_key, partial
from itertools import groupby

from django.conf import settings
from google.cloud.datastore.key import Key

from . import POLYMODEL_CLASS_ATTRIBUTE, caching
from .query_utils import compare_keys, get_filter, is_keys_only
from .utils import django_ordering_comparison, entity_matches_query


class AsyncMultiQuery(object):
    """
        Runs multiple queries simultaneously and merges the result sets based on the
        shared ordering.
    """

    # Testing seems to show that more threads == better, but I'm concerned if we
    # raise this too high we'll start hitting bottlenecks elsewhere. Serious performance
    # testing needs to happen. Theoretically I think we could make THREAD_COUNT == len(queries)
    # but I'd rather prove that doesn't cause problems before I do it!
    THREAD_COUNT = 8

    def __init__(self, queries, orderings):
        self._queries = [copy.copy(x) for x in queries]
        self._orderings = orderings
        self._min_max_cache = {}

        # When set, this is called on the query before .Run() is called
        # Which allows you to manipulate the options. Recommend this is set/unset
        # in a try/finally
        self._query_decorator = None
        self._keys_only = False

    def keys_only(self):
        self._keys_only = True
        for query in self._queries:
            query.keys_only()

    def _spawn_thread(self, i, query, result_queues, **query_run_args):
        """
            Spawns a thread to return a queries resultset

            *Note* by evaluating the entire query results in the thread we ruin the datastore
            query batching in the situation that you:

             a. Have limited the query
             b. Have a large number of results in one or more branches of the OR

            Basically, if you do this:

            MyModel.objects.filter(field1__in=("A", "B"))[:1000]

            and you have 1000 results with "A" and 1000 results with "B" all
            2000 results will be fetched even though you asked for 1000. However, this is
            not the most likely situation for a MultiQuery when normally few results will be returned
            by each branch. Threading seems to help in the common case but we can revisit
            when we have more data. If threading isn't worth the cost we can revert to just using
            async queries like Google's multiquery does.
        """

        keys_only = self._keys_only

        class Thread(threading.Thread):
            def __init__(self, query, *args, **kwargs):
                self.query = query
                self.results_fetched = False
                super(Thread, self).__init__(*args, **kwargs)

            def run(self):
                # Evaluate the result set in the thread, but return an iterator
                # so we can change this if necessary without breaking assumptions elsewhere
                result_queues[i] = (x.key if keys_only else x for x in self.query.fetch(**query_run_args))
                self.results_fetched = True

        if self._query_decorator:
            query = self._query_decorator(query)

        thread = Thread(query)
        thread.start()
        return thread

    def _fetch_results(self, limit=None):
        """
            Returns a list of generators (one for each query in the multi query)
            which generate entity results (or keys if it's keys_only)

            Uses multiple threads to submit RPC calls
        """

        threads = []

        # We need to grab a set of results per query
        result_queues = [None] * len(self._queries)

        # Go through the queries, trigger new threads as they become available
        for i, query in enumerate(self._queries):

            # Iterate while we have a full thread list
            while len(threads) >= self.THREAD_COUNT:
                try:
                    complete = next(x for x in threads if x.results_fetched)
                except StopIteration:
                    # No threads available, continue waiting
                    continue

                # Remove the complete thread
                complete.join()
                threads.remove(complete)

            # Spawn a new thread
            threads.append(self._spawn_thread(i, query, result_queues, limit=limit))

        [x.join() for x in threads]  # Wait until all the threads are done

        return result_queues

    def _compare_entities(self, lhs, rhs):
        def cmp(a, b):
            return (a > b) - (a < b)

        if isinstance(lhs, Key) and isinstance(rhs, Key):
            return compare_keys(lhs, rhs)

        def get_extreme_if_list_property(entity_key, column, value, descending):
            if not isinstance(value, list):
                return value

            if (entity_key, column) in self._min_max_cache:
                return self._min_max_cache[(entity_key, column)]

            if descending:
                value = min(value)
            else:
                value = max(value)
            self._min_max_cache[(entity_key, column)] = value

        if not lhs:
            return cmp(lhs, rhs)

        for column in self._orderings:
            descending = column.startswith("-")
            column = column.lstrip("-")

            lhs_value = lhs.key if column == "__key__" else lhs[column]
            rhs_value = rhs.key if column == "__key__" else rhs[column]

            lhs_value = get_extreme_if_list_property(lhs.key, column, lhs_value, descending)
            rhs_value = get_extreme_if_list_property(lhs.key, column, rhs_value, descending)

            if isinstance(lhs_value, Key) and isinstance(rhs_value, Key):
                result = compare_keys(lhs_value, rhs_value)
            else:
                result = cmp(lhs_value, rhs_value)

            if descending:
                result = -result
            if result:
                return result

        return compare_keys(lhs.key, rhs.key)

    def fetch(self, offset=None, limit=None):
        """
            Returns an iterator through the result set.

            This calls _fetch_results which returns a list of iterators,
            where each is the result of a single query. This function does a
            zig-zag merge of the entities. It starts by creating a list of the next
            entry in each resultset, then iteratively picks the next entity and then
            fills the slot from the counterpart result set until all the slots are None.
        """
        self._min_max_cache = []

        # We have to assume that one branch might return all the results and as
        # offsetting is done by skipping results we need to get offset + limit results
        # from each branch
        results = self._fetch_results(limit=(offset or 0) + limit if limit is not None else None)

        # Go through each outstanding result queue and store
        # the next entry of each (None if the result queue is done)
        next_entries = [None] * len(results)
        for i, queue in enumerate(results):
            try:
                next_entries[i] = next(results[i])
            except StopIteration:
                next_entries[i] = None

        returned_count = 0
        yielded_count = 0

        seen_keys = set()  # For de-duping results
        while any(next_entries):

            def get_next():
                idx, lowest = None, None

                for i, entry in enumerate(next_entries):
                    if entry is None:
                        continue

                    if lowest is None or self._compare_entities(entry, lowest) < 0:
                        idx, lowest = i, entry

                # Move the queue along if we found the entry there
                if lowest is not None:
                    try:
                        next_entries[idx] = next(results[idx])
                    except StopIteration:
                        next_entries[idx] = None

                return lowest

            # Find the next entry from the available queues
            next_entity = get_next()

            # No more entries if this is the case
            if next_entity is None:
                break

            next_key = next_entity if isinstance(next_entity, Key) else next_entity.key

            # Make sure we haven't seen this result before before yielding
            if next_key not in seen_keys:
                returned_count += 1
                seen_keys.add(next_key)

                if offset and returned_count <= offset:
                    # We haven't hit the offset yet, so just
                    # keep fetching entities
                    continue

                yielded_count += 1
                yield next_entity

                if limit and yielded_count == limit:
                    raise StopIteration()


def _convert_entity_based_on_query_options(entity, keys_only, projection):
    if keys_only:
        return entity.key

    if projection:
        keys = list(entity.keys())
        for k in keys:
            if k not in list(projection) + [POLYMODEL_CLASS_ATTRIBUTE]:
                del entity[k]

    return entity


# The max number of entities in a resultset that will be cached
# if a query returns more than this number then only the first ones
# will be cached
DEFAULT_MAX_ENTITY_COUNT = 8


class QueryByKeys(object):
    """ Does the most efficient fetching possible for when we have the keys of the entities we want. """

    def __init__(self, connection, model, queries, ordering, namespace):
        # Imported here for potential circular import and isolation reasons
        from .dnf import DEFAULT_MAX_ALLOWABLE_QUERIES

        # `queries` should be filtered by __key__ with keys that have the namespace applied to them.
        # `namespace` is passed for explicit niceness (mostly so that we don't have to assume that
        # all the keys belong to the same namespace, even though they will).
        def _get_key(query):
            result = get_filter(query, ("__key__", "="))
            return result

        def compare_queries(lhs, rhs):
            return compare_keys(_get_key(lhs), _get_key(rhs))

        self.connection = connection
        self.model = model
        self.namespace = namespace

        # groupby requires that the iterable is sorted by the given key before grouping
        self.queries = sorted(queries, key=cmp_to_key(compare_queries))
        self.query_count = len(self.queries)
        self.queries_by_key = {a: list(b) for a, b in groupby(self.queries, _get_key)}

        self.max_allowable_queries = getattr(settings, "DJANGAE_MAX_QUERY_BRANCHES", DEFAULT_MAX_ALLOWABLE_QUERIES)
        self.can_multi_query = self.query_count < self.max_allowable_queries

        self.ordering = ordering
        self.kind = queries[0].kind
        self._keys_only_override = False

    def keys_only(self):
        self._keys_only_override = True

    def fetch(self, limit=None, offset=None):
        """
            Here are the options:

            1. Single key, hit memcache
            2. Multikey projection, async MultiQueries with ancestors chained
            3. Full select, datastore get
        """
        from gcloudc.db.backends.datastore import transaction

        base_query = self.queries[0]

        is_projection = False

        results = None

        if base_query.projection and self.can_multi_query:
            is_projection = True

            # If we can multi-query in a single query, we do so using a number of
            # ancestor queries (to stay consistent) otherwise, we just do a
            # datastore Get, but this will return extra data over the RPC
            to_fetch = (offset or 0) + limit if limit else None
            additional_cols = set([x[0] for x in self.ordering if x[0] not in base_query.projection])

            multi_query = []
            orderings = base_query.order
            for key, queries in self.queries_by_key.items():
                for query in queries:
                    if additional_cols:
                        # We need to include additional orderings in the projection so that we can
                        # sort them in memory. Annoyingly that means reinstantiating the queries
                        query = rpc.Query(
                            kind=query._Query__kind,
                            filters=query,
                            projection=list(base_query.projection).extend(list(additional_cols)),
                            namespace=self.namespace,
                        )

                    query.ancestor = key  # Make this an ancestor query
                    multi_query.append(query)

            if len(multi_query) == 1:
                results = multi_query[0].fetch(limit=to_fetch)
            else:
                results = AsyncMultiQuery(multi_query, orderings).fetch(limit=to_fetch)
        else:
            results = transaction._rpc(self.connection).get([x for x in self.queries_by_key.keys()])

        def iter_results(results):
            returned = 0
            # This is safe, because Django is fetching all results any way :(
            sorted_results = sorted(results, key=cmp_to_key(partial(django_ordering_comparison, self.ordering)))
            sorted_results = [result for result in sorted_results if result is not None]

            for result in sorted_results:
                if is_projection:
                    matches_query = True
                else:
                    matches_query = any(entity_matches_query(result, qry) for qry in self.queries_by_key[result.key])

                if not matches_query:
                    continue

                if offset and returned < offset:
                    # Skip entities based on offset
                    returned += 1
                    continue
                else:
                    yield _convert_entity_based_on_query_options(
                        result, self._keys_only_override or is_keys_only(base_query), base_query.projection
                    )

                    returned += 1

                    # If there is a limit, we might be done!
                    if limit is not None and returned == (offset or 0) + limit:
                        break

        return iter_results(results)


class NoOpQuery(object):
    def fetch(self, limit, offset):
        return []


class UniqueQuery(object):
    """
        This mimics a normal query but hits the cache if possible. It must
        be passed the set of unique fields that form a unique constraint
    """

    def __init__(self, unique_identifier, gae_query, model, namespace):
        self._identifier = unique_identifier
        self._gae_query = gae_query
        self._model = model
        self._namespace = namespace

        self._Query__kind = gae_query._Query__kind

    def get(self, x):
        return self._gae_query.get(x)

    def keys(self):
        return self._gae_query.keys()

    def fetch(self, limit, offset):
        opts = self._gae_query._Query__query_options
        if opts.keys_only or opts.projection:
            return self._gae_query.Run(limit=limit, offset=offset)

        ret = caching.get_from_cache(self._identifier, self._namespace)
        if ret is not None and not entity_matches_query(ret, self._gae_query):
            ret = None

        if ret is None:
            # We do a fast keys_only query to get the result
            keys_query = rpc.Query(self._gae_query._Query__kind, keys_only=True, namespace=self._namespace)
            keys_query.update(self._gae_query)
            keys = keys_query.Run(limit=limit, offset=offset)

            # Do a consistent get so we don't cache stale data, and recheck the result matches the query
            ret = [x for x in rpc.Get(keys) if x and entity_matches_query(x, self._gae_query)]
            if len(ret) == 1:
                caching.add_entities_to_cache(
                    self._model, [ret[0]], caching.CachingSituation.DATASTORE_GET, self._namespace
                )
            return iter(ret)

        return iter([ret])
