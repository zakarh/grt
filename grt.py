# Authors:
# - Zakar Handricken
import os
import json


class EdgeManager:
    def __init__(self, storage_dir: str = "edges"):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.sep = "♥"

    def _edge_path(self, src: str, dest: str) -> str:
        return os.path.join(self.storage_dir, f"{src}{self.sep}{dest}.json")

    def create(self, src: str, dest: str, properties: dict = {}) -> dict:
        edge_path = self._edge_path(src, dest)
        if not os.path.exists(edge_path):
            with open(edge_path, "w") as f:
                json.dump(properties, f)

    def get(self, src: str, dest: str) -> dict:
        edge_path = self._edge_path(src, dest)
        if os.path.exists(edge_path):
            with open(edge_path, "r") as f:
                return json.load(f)
        return None

    def update(self, src: str, dest: str, properties):
        edge_path = self._edge_path(src, dest)
        if os.path.exists(edge_path):
            with open(edge_path, "w") as f:
                json.dump(properties, f)

    def delete(self, src: str, dest: str):
        edge_path = self._edge_path(src, dest)
        if os.path.exists(edge_path):
            os.remove(edge_path)

    def delete_related(self, key: str):
        for f in os.listdir(self.storage_dir):
            if f.endswith(".json"):
                if f[: len(f) - 5].split(self.sep)[1] == key:
                    src = f[: len(f) - 5].split(self.sep)[0]
                    self.delete(src, key)
                if f[: len(f) - 5].split(self.sep)[0] == key:
                    dest = f[: len(f) - 5].split(self.sep)[1]
                    self.delete(key, dest)

    def contains(self, src: str, dest: str) -> bool:
        return os.path.exists(self._edge_path(src, dest))

    def all(self) -> iter:
        for f in os.listdir(self.storage_dir):
            if f.endswith(".json"):
                yield tuple(f[: len(f) - 5].split(self.sep))

    def incoming(self, key: str) -> iter:
        for f in os.listdir(self.storage_dir):
            if f.endswith(".json"):
                if f[: len(f) - 5].split(self.sep)[1] == key:
                    yield f[: len(f) - 5].split(self.sep)[0]

    def outgoing(self, key: str) -> iter:
        for f in os.listdir(self.storage_dir):
            if f.endswith(".json"):
                if f[: len(f) - 5].split(self.sep)[0] == key:
                    yield f[: len(f) - 5].split(self.sep)[1]


class NodeManager:
    def __init__(self, edge_manager: EdgeManager, storage_dir: str = "nodes"):
        self.edges: EdgeManager = edge_manager
        self.storage_dir: str = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def _node_path(self, key: str):
        return os.path.join(self.storage_dir, f"{key}.json")

    def create(self, key: str, properties: dict = {}):
        node_path = self._node_path(key)
        if not os.path.exists(node_path):
            with open(node_path, "w") as f:
                json.dump(properties, f)

    def get(self, key: str) -> dict:
        node_path: str = self._node_path(key)
        if os.path.exists(node_path):
            with open(node_path, "r") as f:
                return json.load(f)
        return None

    def update(self, key: str, properties):
        node_path = self._node_path(key)
        if os.path.exists(node_path):
            with open(node_path, "w") as f:
                json.dump(properties, f)

    def delete(self, key: str):
        node_path = self._node_path(key)
        if os.path.exists(node_path):
            os.remove(node_path)
        self.edges.delete_related(key)
        # for into in self.edges.incoming(key):
        #     self.edges.delete(into, key)
        # for out_from in self.edges.outgoing(key):
        #     self.edges.delete(key, out_from)

    def contains(self, key: str) -> bool:
        return os.path.exists(self._node_path(key))

    def all(self) -> iter:
        for f in os.listdir(self.storage_dir):
            if f.endswith(".json"):
                yield f[: len(f) - 5]

    def keys(self) -> list:
        return self.all()


class GRT(object):
    def __init__(
        self,
        directory="./graph",
    ) -> None:
        self.edges = EdgeManager(os.path.join(directory, "edges"))
        self.nodes = NodeManager(self.edges, os.path.join(directory, "nodes"))


if __name__ == "__main__":
    grt = GRT()

    # Create nodes
    grt.nodes.create("1", {"name": "Node 1"})
    grt.nodes.create("2", {"name": "Node 2"})

    # Create an edge
    grt.edges.create("1", "2", {"relationship": "connected"})

    # Get node
    print("Node 1:", grt.nodes.get("1"))
    print("Node All:", next(grt.nodes.all()))

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

    # Delete nodes and edges
    grt.nodes.delete("1")
    grt.edges.delete("1", "2")
    grt.nodes.delete("2")
