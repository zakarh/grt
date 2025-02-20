# Authors:
# - Zakar Handricken

import os
import json
from pathlib import Path


class EdgeManager:
    def __init__(self, storage_dir="edges"):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.sep = "♥"

    def _edge_path(self, src, dest):
        return os.path.join(self.storage_dir, f"{src}{self.sep}{dest}.json")

    def create(self, src, dest, properties=None):
        if properties is None:
            properties = {}
        edge_path = self._edge_path(src, dest)
        if not os.path.exists(edge_path):
            with open(edge_path, "w") as f:
                json.dump(properties, f)

    def get(self, src, dest):
        edge_path = self._edge_path(src, dest)
        if os.path.exists(edge_path):
            with open(edge_path, "r") as f:
                return json.load(f)
        return None

    def update(self, src, dest, properties):
        edge_path = self._edge_path(src, dest)
        if os.path.exists(edge_path):
            with open(edge_path, "w") as f:
                json.dump(properties, f)

    def delete(self, src, dest):
        edge_path = self._edge_path(src, dest)
        if os.path.exists(edge_path):
            os.remove(edge_path)

    def contains(self, src, dest):
        return os.path.exists(self._edge_path(src, dest))

    def all(self):
        return [
            tuple(f.split(".")[0].split(self.sep))
            for f in os.listdir(self.storage_dir)
            if f.endswith(".json")
        ]

    def incoming(self, key):
        return [
            f.split(self.sep)[0]
            for f in os.listdir(self.storage_dir)
            if f.endswith(".json") and f.split(self.sep)[1].replace(".json", "") == key
        ]

    def outgoing(self, key):
        return [
            f.split(self.sep)[1].replace(".json", "")
            for f in os.listdir(self.storage_dir)
            if f.endswith(".json") and f.split(self.sep)[0] == key
        ]


class NodeManager:
    def __init__(self, edge_manager: EdgeManager, storage_dir="nodes"):
        self.edges = edge_manager
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def _node_path(self, key):
        return os.path.join(self.storage_dir, f"{key}.json")

    def create(self, key, properties=None):
        if properties is None:
            properties = {}
        node_path = self._node_path(key)
        if not os.path.exists(node_path):
            with open(node_path, "w") as f:
                json.dump(properties, f)

    def get(self, key):
        node_path = self._node_path(key)
        if os.path.exists(node_path):
            with open(node_path, "r") as f:
                return json.load(f)
        return None

    def update(self, key, properties):
        node_path = self._node_path(key)
        if os.path.exists(node_path):
            with open(node_path, "w") as f:
                json.dump(properties, f)

    def delete(self, key):
        node_path = self._node_path(key)
        if os.path.exists(node_path):
            os.remove(node_path)
        for into in self.edges.incoming(key):
            self.edges.delete(into, key)
        for outfrom in self.edges.outgoing(key):
            self.edges.delete(key, outfrom)

    def contains(self, key):
        return os.path.exists(self._node_path(key))

    def all(self):
        return [
            f.split(".")[0] for f in os.listdir(self.storage_dir) if f.endswith(".json")
        ]

    def keys(self):
        return self.all()


class GRT(object):
    def __init__(
        self,
        directory="./graph",
    ) -> None:
        self.edges = EdgeManager(Path(directory).joinpath("edges"))
        self.nodes = NodeManager(self.edges, Path(directory).joinpath("nodes"))

if __name__ == "__main__":
    grt = GRT()
    # Initialize Node and Edge Managers
    # grt.edges = EdgeManager()
    # grt.nodes = NodeManager(grt.edges)

    # Create nodes

    # import time
    # start = time.time()
    # for i in range(100_000):
    #     node_manager.create(i, {"name": "Node 1"})
    # print("total_time:", time.time() - start)
    # exit()

    grt.nodes.create("1", {"name": "Node 1"})
    grt.nodes.create("2", {"name": "Node 2"})
    input()
    # Create an edge
    grt.edges.create("1", "2", {"relationship": "connected"})

    # Get node
    print("Node 1:", grt.nodes.get("1"))

    # Get edge
    print("Edge 1->2:", grt.edges.get("1", "2"))

    # Check if node exists
    print("Node 1 exists:", grt.nodes.contains("1"))

    # Check if edge exists
    print("Edge 1->2 exists:", grt.edges.contains("1", "2"))

    # Update node
    grt.nodes.update("1", {"name": "Updated Node 1"})
    print("Updated Node 1:", grt.nodes.get("1"))

    # Update edge
    grt.edges.update("1", "2", {"relationship": "updated connection"})
    print("Updated Edge 1->2:", grt.edges.get("1", "2"))
    input()
    
    # Delete node and edge
    grt.nodes.delete("1")
    grt.edges.delete("1", "2")
    print("Node 1 after deletion:", grt.nodes.get("1"))
    print("Edge 1->2 after deletion:", grt.edges.get("1", "2"))
