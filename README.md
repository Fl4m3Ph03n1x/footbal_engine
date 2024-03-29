# FootbalEngine

## What is it

The FootbalEngine consists of a project that emulates an in memory database with
indexed keys. The idea of the engine is to be a self sustained, resilient source
of data that others applications can import and then use.

The engine makes heavy use of memory, if you have a CSV file with `n` columns
and `m` rows, the expected memory usage complexity will be around `O(n*m)`, as
each row is indexed for faster search queries.

## How to use it

You can launch the engine by running `iex -S mix`.

The public API this Engine offers can be accessed via the `FootbalEngine` module.
You can check the [official documentation](https://fl4m3ph03n1x.github.io/footbal_engine/api-reference.html) for more information.
