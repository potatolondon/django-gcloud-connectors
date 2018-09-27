import copy
import threading

from functools import partial
from itertools import groupby

from django.conf import settings
from djangae.db.backends.appengine import caching
from djangae.db import utils

from djangae.db.backends.appengine import POLYMODEL_CLASS_ATTRIBUTE

from google.appengine.api import datastore
from google.appengine.api.datastore import Query
from google.appengine.datastore.datastore_query import QueryOptions


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
        self._queries = queries
        self._orderings = orderings
        self._min_max_cache = {}

        # When set, this is called on the query before .Run() is called
        # Which allows you to manipulate the options. Recommend this is set/unset
        # in a try/finally
        self._query_decorator = None

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

        class Thread(threading.Thread):
            def __init__(self, query, *args, **kwargs):
                self.query = query
                self.results_fetched = False
                super(Thread, self).__init__(*args, **kwargs)

            def run(self):
                # Evaluate the result set in the thread, but return an iterator
                # so we can change this if necessary without breaking assumptions elsewhere
                result_queues[i] = iter([x for x in query.Run(**query_run_args)])
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
                    complete = (x for x in threads if x.results_fetched).next()
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
        if all([isinstance(x, datastore.Key) for x in (lhs, rhs)]):
            return cmp(lhs, rhs)

        def get_extreme_if_list_property(entity_key, column, value, order):
            if not isinstance(value, list):
                return value

            if (entity_key, column) in self._min_max_cache:
                return self._min_max_cache[(entity_key, column)]

            if order == Query.DESCENDING:
                value = min(value)
            else:
                value = max(value)
            self._min_max_cache[(entity_key, column)] = value

        if not lhs:
            return cmp(lhs, rhs)

        for column, order in self._orderings:
            lhs_value = lhs.key() if column == "__key__" else lhs[column]
            rhs_value = rhs.key() if column == "__key__" else rhs[column]

            lhs_value = get_extreme_if_list_property(lhs.key(), column, lhs_value, order)
            rhs_value = get_extreme_if_list_property(lhs.key(), column, rhs_value, order)

            result = cmp(lhs_value, rhs_value)

            if order == Query.DESCENDING:
                result = -result
            if result:
                return result

        return cmp(lhs.key(), rhs.key())

    def Count(self, **kwargs):
        def query_decorator(query):
            # Force keys_only
            # we copy to prevent changing the original query
            # unexpectedly
            query = copy.deepcopy(query)
            query._Query__query_options = QueryOptions(keys_only=True)

            return query

        try:
            self._query_decorator = query_decorator
            return len([x for x in self.Run(**kwargs)])
        finally:
            self._query_decorator = None

    def Run(self, offset=None, limit=None):
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
        results = self._fetch_results(
            limit=(offset or 0) + limit if limit is not None else None
        )

        # Go through each outstanding result queue and store
        # the next entry of each (None if the result queue is done)
        next_entries = [None] * len(results)
        for i, queue in enumerate(results):
            try:
                next_entries[i] = results[i].next()
            except StopIteration:
                next_entries[i] = None

        counters = {
            'returned': 0,
            'yielded': 0
        }

        seen_keys = set()  #For de-duping results
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
                        next_entries[idx] = results[idx].next()
                    except StopIteration:
                        next_entries[idx] = None

                return lowest

            # Find the next entry from the available queues
            next_entity = get_next()

            # No more entries if this is the case
            if next_entity is None:
                break

            next_key = (
                next_entity
                if isinstance(next_entity, datastore.Key) else next_entity.key()
            )

            # Make sure we haven't seen this result before before yielding
            if next_key not in seen_keys:
                counters['returned'] += 1

                if offset and counters['returned'] <= offset:
                    # We haven't hit the offset yet, so just
                    # keep fetching entities
                    continue

                seen_keys.add(next_key)
                counters['yielded'] += 1
                yield next_entity

                if limit and counters['yielded'] == limit:
                    raise StopIteration()


def _convert_entity_based_on_query_options(entity, opts):
    if opts.keys_only:
        return entity.key()

    if opts.projection:
        for k in entity.keys()[:]:
            if k not in list(opts.projection) + [POLYMODEL_CLASS_ATTRIBUTE]:
                del entity[k]

    return entity


# The max number of entities in a resultset that will be cached
# if a query returns more than this number then only the first ones
# will be cached
DEFAULT_MAX_ENTITY_COUNT = 8


class QueryByKeys(object):
    """ Does the most efficient fetching possible for when we have the keys of the entities we want. """

    def __init__(self, model, queries, ordering, namespace):
        # Imported here for potential circular import and isolation reasons
        from djangae.db.backends.appengine.dnf import DEFAULT_MAX_ALLOWABLE_QUERIES

        # `queries` should be filtered by __key__ with keys that have the namespace applied to them.
        # `namespace` is passed for explicit niceness (mostly so that we don't have to assume that
        # all the keys belong to the same namespace, even though they will).
        def _get_key(query):
            result = query["__key__ ="]
            return result

        self.model = model
        self.namespace = namespace

        # groupby requires that the iterable is sorted by the given key before grouping
        self.queries = sorted(queries, key=_get_key)
        self.query_count = len(self.queries)
        self.queries_by_key = {a: list(b) for a, b in groupby(self.queries, _get_key)}

        self.max_allowable_queries = getattr(
            settings, "DJANGAE_MAX_QUERY_BRANCHES", DEFAULT_MAX_ALLOWABLE_QUERIES
        )
        self.can_multi_query = self.query_count < self.max_allowable_queries

        self.ordering = ordering
        self._Query__kind = queries[0]._Query__kind

    def Run(self, limit=None, offset=None):
        """
            Here are the options:

            1. Single key, hit memcache
            2. Multikey projection, async MultiQueries with ancestors chained
            3. Full select, datastore get
        """
        opts = self.queries[0]._Query__query_options
        key_count = len(self.queries_by_key)

        is_projection = False

        max_cache_count = getattr(
            settings, "DJANGAE_CACHE_MAX_ENTITY_COUNT", DEFAULT_MAX_ENTITY_COUNT
        )

        cache_results = True
        results = None
        if key_count == 1:
            # FIXME: Potentially could use get_multi in memcache and the make a query
            # for whatever remains
            key = self.queries_by_key.keys()[0]
            result = caching.get_from_cache_by_key(key)
            if result is not None:
                results = [result]
                cache_results = False  # Don't update cache, we just got it from there

        if results is None:
            if opts.projection and self.can_multi_query:
                is_projection = True
                cache_results = False  # Don't cache projection results!

                # If we can multi-query in a single query, we do so using a number of
                # ancestor queries (to stay consistent) otherwise, we just do a
                # datastore Get, but this will return extra data over the RPC
                to_fetch = (offset or 0) + limit if limit else None
                additional_cols = set([x[0] for x in self.ordering if x[0] not in opts.projection])

                multi_query = []
                orderings = self.queries[0]._Query__orderings
                for key, queries in self.queries_by_key.items():
                    for query in queries:
                        if additional_cols:
                            # We need to include additional orderings in the projection so that we can
                            # sort them in memory. Annoyingly that means reinstantiating the queries
                            query = Query(
                                kind=query._Query__kind,
                                filters=query,
                                projection=list(opts.projection).extend(list(additional_cols)),
                                namespace=self.namespace,
                            )

                        query.Ancestor(key)  # Make this an ancestor query
                        multi_query.append(query)

                if len(multi_query) == 1:
                    results = multi_query[0].Run(limit=to_fetch)
                else:
                    results = AsyncMultiQuery(multi_query, orderings).Run(limit=to_fetch)
            else:
                results = datastore.Get(self.queries_by_key.keys())

        def iter_results(results):
            returned = 0
            # This is safe, because Django is fetching all results any way :(
            sorted_results = sorted(
                results, 
                cmp=partial(utils.django_ordering_comparison, self.ordering)
            )
            sorted_results = [result for result in sorted_results if result is not None]
            if cache_results and sorted_results:
                caching.add_entities_to_cache(
                    self.model,
                    sorted_results[:max_cache_count],
                    caching.CachingSituation.DATASTORE_GET,
                    self.namespace,
                )

            for result in sorted_results:
                if is_projection:
                    entity_matches_query = True
                else:
                    entity_matches_query = any(
                        utils.entity_matches_query(result, qry)
                        for qry in self.queries_by_key[result.key()]
                    )

                if not entity_matches_query:
                    continue

                if offset and returned < offset:
                    # Skip entities based on offset
                    returned += 1
                    continue
                else:

                    yield _convert_entity_based_on_query_options(result, opts)

                    returned += 1

                    # If there is a limit, we might be done!
                    if limit is not None and returned == (offset or 0) + limit:
                        break

        return iter_results(results)

    def Count(self, limit, offset):
        return len([x for x in self.Run(limit, offset)])


class NoOpQuery(object):
    def Run(self, limit, offset):
        return []

    def Count(self, limit, offset):
        return 0


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

    def Run(self, limit, offset):
        opts = self._gae_query._Query__query_options
        if opts.keys_only or opts.projection:
            return self._gae_query.Run(limit=limit, offset=offset)

        ret = caching.get_from_cache(self._identifier, self._namespace)
        if ret is not None and not utils.entity_matches_query(ret, self._gae_query):
            ret = None

        if ret is None:
            # We do a fast keys_only query to get the result
            keys_query = Query(
                self._gae_query._Query__kind, keys_only=True, namespace=self._namespace
            )
            keys_query.update(self._gae_query)
            keys = keys_query.Run(limit=limit, offset=offset)

            # Do a consistent get so we don't cache stale data, and recheck the result matches the query
            ret = [
                x for x in datastore.Get(keys) 
                if x and utils.entity_matches_query(x, self._gae_query)
            ]
            if len(ret) == 1:
                caching.add_entities_to_cache(
                    self._model,
                    [ret[0]],
                    caching.CachingSituation.DATASTORE_GET,
                    self._namespace,
                )
            return iter(ret)

        return iter([ret])

    def Count(self, limit, offset):
        return sum(1 for x in self.Run(limit, offset))


