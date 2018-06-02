To test add the `hn_logs.tsv` file to the directory and run `python3.6 server.py`

# Thought process

## Assignment
- The TSV file looks like it has only two columns: search_timestamp & search_query
- The good practice would be to put that into a SQL database (potentially not the best performance but likely good enough and it easy to develop, maintain, and evolve features)
- And of course if the problem came up at work and a database was not possible I would ask people/experts before trying myself... but this time I'm on my own :) 
- The only queries needed are:
  1. count distinct search_queries by date prefix 
  2. most popular search_queries by date prefix
  
## Data structure
- We're always working with date prefix so the most promising data structure is a Trie where we can cache stuff in the nodes.
- It's probably better not to rely on an ordering of the timestamps for more robustness, being able to replay log files, etc...  
- We cannot use a heap to keep track of the top queries because the count for a given query is always changing...
- In the end I used a cache of counts per query per prefix(only during Trie construction) and a cache of the top k queries per node. 
  This means we cannot get an arbitrary number of most popular queries, it has to be limit e.g. k < 50, but this seemed an acceptable compromise.
- The obvious advantage is that we can answer queries quickly => O(1)
- But the space complexity is large and it will run into memory problems at scale, although part of the data could be offloaded to disk with a few changes.
- There is probably a better solution...

## Notes
- Looks like we want case-sensitive aggregations because forcing lowercase on the search_query gives different counts
- BaseHTTPRequestHandler is a very basic web server but it seems appropriate for this kind of short assignment
