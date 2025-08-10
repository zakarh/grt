# grt: Graph Representation Tool

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
| Create Nodes      | 5s    |
| Create Edges      | 5s    |
| Query Nodes       | 0s    |
| Query Edges       | 0s    |
| Updating Nodes    | 5s    |
| Deleting Nodes    | 1m32s |

For 100,000 Nodes and 100,000 edges.

|                   | Time  |
|---                |---    |
| Create Nodes      | 1m00s |
| Create Edges      | 1m02s |
| Query Nodes       | 0s    |
| Query Edges       | 0s    |
| Updating Nodes    | 55s   |
| Deleting Nodes    | 2h18m42s |


### HARD DRIVE SPECS

**Micron MTFDKCD512QGN-1BN1AABLA**  
**Read speed:** 16.2 KB/s  
**Write speed:** 1.2 MB/s  
