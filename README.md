# grt

A simple graph database system. It provides functionality to manage nodes and edges,
making it useful for building systems that require lightweight graph-like data storage

## Using the tool.

    from grt_<type> import GRT
    graph = GRT()
    

## To run tests.

    pip install -r requirements-dev.txt
    pytest

## To run benchmarks.

    python benchmark_<type>.py

For 10,000 Nodes and 10,000 edges.

|                   | File  | SQLite    | Partition   |
|---                |---    |---        |---          |
| Create Nodes      | 6s    | 1m        | 1m05s       |
| Create Edges      | 6s    | 1m        | 2m03s       |
| Query Nodes       | 0s    | 0s        | 0s          |
| Query Edges       | 0s    | 0s        | 0s          |
| Updating Nodes    | 11s   | 58s       | 1m19s       |
| Deleting Nodes    | 1m17s | 2m04s     | 3m16s       |
