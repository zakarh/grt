# grt

A simple graph database system. It provides functionality to manage nodes and edges,
making it useful for building systems that require lightweight graph-like data storage

## Using the tool.

    from grt import GRT
    graph = GRT()
    

## To run tests.

    pip install -r requirements-test.txt
    pytest test.py

## To run benchmarks.

    pip install -r requirements-benchmark.txt
    python benchmark.py

For 10,000 Nodes and 10,000 edges.

|                   | File  |
|---                |---    |
| Create Nodes      | 6s    |
| Create Edges      | 6s    |
| Query Nodes       | 0s    |
| Query Edges       | 0s    |
| Updating Nodes    | 11s   |
| Deleting Nodes    | 1m17s |
