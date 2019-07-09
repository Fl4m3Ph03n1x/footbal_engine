# FootbalEngine

The FootbalEngine consists of a project that emulates an in memory database with
indexed keys. The idea of the engine is to be a self sustained, resilient source 
of data that others applications can import and then use. 

The engine makes heavy use of memory, if you have a CSV file with `n` columns
and `m` rows, the expected memory usage complexity will be around `O(n*m)`, as
each row is indexed for faster search queries.

# footbal_engine

The public API this Engine offers can be accessed via the `QuickSearch` module.
You can check the documenation for this module in out Github Pages section:

