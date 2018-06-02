import logging
from collections import namedtuple


QueryData = namedtuple("QueryData", ["query_text", "prefix_count"])
TopQuery = namedtuple("TopQuery", ["id", "count"])
TOP_QUERIES_SIZE_K = 50


class QueryStore(object):
    """Store information about queries in a list.
    The query id is the index of the query in this list
    The stored data for each query is the tuple (query_text, prefix_count)
    The prefix_count is dictionary containing the number of times the query appears at
    each prefix. It is used when processing the logs but can be discarded after.
    """
    def __init__(self):
        self.queries_data = []
        self.text_index = {}
        self.finalized = False

    def add(self, query_text):
        if self.finalized:
            raise RuntimeError("Cannot add query info to the QueryStore because it is finalized")
        if query_text in self.text_index:
            return self.text_index[query_text]
        query_id = len(self.queries_data)
        self.queries_data.append(QueryData(query_text, {}))
        self.text_index[query_text] = query_id
        return query_id

    def get(self, query_id):
        return self.queries_data[query_id]

    def finalize(self):
        """Free up memory once aggregations are done"""
        self.text_index.clear()
        for qd in self.queries_data:
            qd.prefix_count.clear()
        self.finalized = True


class TopQueriesCache(object):
    """Cache the top k most popular queries in a list of (query_id, query_count) tuples
    ordered by query_count"""
    def __init__(self):
        self.top_queries = []

    def find_query(self, query_id):
        """This is O(k) but could be optimized by using the query count to find
        the entry faster in the sorted array"""
        for idx, tq in enumerate(self.top_queries):
            if tq.id == query_id:
                return idx
        return None

    def update(self, query_id, query_count):
        """Update the top_queries given the current query info
        This is O(k log k) in the worst case, O(k) on average, could be improved(find_query)
        """
        # If the query is already in our cache, update the count and sort
        idx = self.find_query(query_id)
        if idx is not None:
            assert self.top_queries[idx].id == query_id
            assert self.top_queries[idx].count == query_count - 1
            self.top_queries[idx] = TopQuery(query_id, query_count)
            self.top_queries.sort(key=lambda tq: -tq.count)
            return

        # The query is not yet in our cache
        # If the cache is not full, just append the data
        if len(self.top_queries) < TOP_QUERIES_SIZE_K:
            self.top_queries.append(TopQuery(query_id, query_count))
            self.top_queries.sort(key=lambda tq: -tq.count)
            return

        # We only need to add the query if the count is better than last_top_query
        last_top_query = self.top_queries[-1]
        if query_count > last_top_query.count:
            self.top_queries[-1] = TopQuery(query_id, query_count)
            self.top_queries.sort(key=lambda tq: -tq.count)

    def get(self, limit):
        return self.top_queries[:limit]


class TrieNode(object):
    def __init__(self):
        # There are 10 children, one for each digit
        self.children = [None] * 10
        # Cache the number of distinct queries for this prefix
        self.distinct = 0
        # Cache the top queries for this prefix
        self.top_queries_cache = TopQueriesCache()


class Trie(object):
    """Trie of timestamps
    Insertion and query of timestamp prefix is very fast but there is a large space
    complexity: years * 12 * 31 * 24 * 60 * 60 potential leaves(a bit more nodes),
    and at each node we store pointers for the top k queries"""
    def __init__(self):
        self.root = TrieNode()
        self.query_store = QueryStore()

    def add(self, timestamp, query_text):
        """Record a query in our Trie structure"""
        time_digits = [int(c) for c in timestamp if c.isdigit()]
        assert len(time_digits) == 14

        query_id = self.query_store.add(query_text)
        prefix_count = self.query_store.get(query_id).prefix_count

        # Do a Trie traversal and update each node with the query info
        node = self.root
        prefix = ""
        for digit in time_digits:
            prefix += str(digit)

            # Search for the digit in the children of the current node
            if node.children[digit] is None:
                # No child yet for this prefix, create one
                new_node = TrieNode()
                node.children[digit] = new_node
            node = node.children[digit]

            # If this is the first time we get this query at this node, increment the distinct queries counter
            if prefix not in prefix_count:
                prefix_count[prefix] = 1
                node.distinct += 1
            else:
                prefix_count[prefix] += 1

            # Update the node top queries
            node.top_queries_cache.update(query_id, prefix_count[prefix])

    def get_node_at_prefix(self, prefix):
        """Return the node in our Trie corresponding to the given prefix, or None if missing.
        prefix is assumed to be a timestamp like 2016-02-10 11:03:50 truncated
        at any point, e.g. 2016, 2016-02, ...
        """
        node = self.root
        prefix_digits = [int(c) for c in prefix if c.isdigit()]
        if not prefix_digits:
            raise InvalidDatePrefix(f"The date prefix '{prefix}' is invalid")

        for digit in prefix_digits:
            node = node.children[digit]
            if node is None:
                return None
        return node

    def finalize(self):
        """Free up memory once aggregations are done"""
        self.query_store.finalize()

    def top_queries_by_prefix(self, prefix, size):
        logging.info(f"Get the top {size} popular queries that have been done with date prefix {prefix}.")
        if size > TOP_QUERIES_SIZE_K:
            raise InvalidQuerySize(f"The maximum size supported for this query is {TOP_QUERIES_SIZE_K}")
        node = self.get_node_at_prefix(prefix)
        if not node:
            return []
        top_queries = node.top_queries_cache.get(size)
        # Replace the query_ids with the query_text before returning
        return [(self.query_store.get(tq.id).query_text, tq.count) for tq in top_queries]

    def distinct_queries_by_prefix(self, prefix):
        logging.info(f"Get the number of distinct queries that have been done with date prefix {prefix}.")
        node = self.get_node_at_prefix(prefix)
        if not node:
            return 0
        return node.distinct


class InvalidQuerySize(Exception):
    pass


class InvalidDatePrefix(Exception):
    pass


if __name__ == "__main__":
    # Run some tests
    trie = Trie()
    trie.add("2014-08-01 00:03:49", "vungle")
    trie.add("2015-09-01 00:03:49", "vungle")
    trie.add("2015-08-01 00:03:49", "test")
    trie.add("2015-11-01 00:03:49", "test")

    assert trie.distinct_queries_by_prefix('2015') == 2
    assert trie.distinct_queries_by_prefix('2015-08') == 1
    assert trie.distinct_queries_by_prefix('2013') == 0

    assert trie.top_queries_by_prefix('2015', 2)[0] == ("test", 2)
    assert trie.top_queries_by_prefix('2015', 2)[1] == ("vungle", 1)
    assert trie.top_queries_by_prefix('2013', 2) == []