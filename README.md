# grt

A lightweight, disk-backed graph database. It provides functionality to manage nodes and edges,
making it useful for building systems that require lightweight graph-like data storage

## Using the tool.

    from grt import GRT
    graph = GRT()
    

## To run tests.

    pip install -r requirements.test.txt
    pytest test.py

## To run benchmarks.

    pip install -r requirements.benchmark.txt
    python benchmark.py

For 10,000 Nodes and 10,000 edges.

|                   | Time  |
|---                |---    |
| Create Nodes      | 7s    |
| Create Edges      | 7s    |
| Query Nodes       | 0s    |
| Query Edges       | 0s    |
| Updating Nodes    | 5s    |
| Deleting Nodes    | 3m14s |

For 100,000 Nodes and 100,000 edges.

|                   | Time  |
|---                |---    |
| Create Nodes      | 1m46s |
| Create Edges      | 1m44s |
| Query Nodes       | 0s    |
| Query Edges       | 0s    |
| Updating Nodes    | 1m02s |
| Deleting Nodes    | ????? |
