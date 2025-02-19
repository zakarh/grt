# Authors:
# - Zakar Handricken
import copy
import cProfile
import glob
import json
import shutil
import traceback
from pathlib import Path
from string import ascii_lowercase, ascii_uppercase, digits, punctuation

extract = lambda x: int(str(x).split("-")[-1].split(".")[0])


class Node(object):
    __slots__ = ["key", "properties"]

    def __init__(
        self,
        key=None,
        properties=None,
    ) -> None:
        self.key = key
        self.properties = properties

    def update(self, state: dict) -> None:
        self.key = state["key"]
        self.properties = state["properties"]

    def data(self) -> dict:
        return {"key": self.key, "properties": self.properties}


class Edge(object):
    __slots__ = ["source", "target", "properties"]

    def __init__(
        self,
        source=None,
        target=None,
        properties=None,
    ) -> None:
        self.source = source
        self.target = target
        self.properties = properties

    def update(self, state: dict) -> None:
        self.source = state["source"]
        self.target = state["target"]
        self.properties = state["properties"]

    def data(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "properties": self.properties,
        }


class NodePartition:
    __slots__ = [
        "directory",
        "capacity",
        "bucket",
        "size",
        "partition_no",
        "nodes",
    ]

    def __init__(
        self,
        directory: str = ".",
        bucket: str = "",
        capacity: int = 1000,
        partition_no: int = 0,
    ) -> None:
        self.directory = directory
        self.bucket = bucket if bucket is not None else []
        self.capacity = capacity
        self.partition_no = partition_no
        self.size = 0
        self.nodes = {}

        Path(self.directory).mkdir(parents=True, exist_ok=True)

    def update(self, state: dict):
        self.directory = state["directory"]
        self.bucket = state["bucket"]
        self.capacity = state["capacity"]
        self.size = state["size"]
        self.partition_no = state["partition_no"]
        self.nodes = state["nodes"]

    def data(self):
        return {
            "directory": str(self.directory),
            "bucket": self.bucket,
            "capacity": self.capacity,
            "size": self.size,
            "partition_no": self.partition_no,
            "nodes": self.nodes,
        }

    def load(self, partition):
        with open(partition, "r") as f:
            self.update(json.load(f))

    def dump(self):
        filepath = (
            Path(self.directory)
            .joinpath(*self.bucket)
            .joinpath(f"node-partition-{self.partition_no}.json")
        )
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(self.data(), f)

    def partitions(self, buckets=None, reverse=False):
        if buckets:
            partitions = sorted(
                [*Path(self.directory).joinpath(*buckets).glob("*.json")],
                key=extract,
                reverse=True,
            )
        else:
            partitions = sorted(
                [*Path(self.directory).glob("./**/*.json")],
                key=extract,
                reverse=True,
            )
        return partitions

    def clear(self):
        self.nodes = {}
        self.partition_no = 0
        self.size = 0
        self.bucket = []


class EdgePropertyPartition:
    __slots__ = [
        "directory",
        "capacity",
        "bucket",
        "size",
        "partition_no",
        "properties",
        "relations",
    ]

    def __init__(
        self,
        directory: str = ".",
        bucket: list[str] = [],
        capacity: int = 1000,
        partition_no: int = 0,
    ) -> None:
        self.directory = directory
        self.bucket = bucket
        self.capacity = capacity
        self.partition_no = partition_no
        self.size = 0
        self.properties = {}

        Path(self.directory).mkdir(parents=True, exist_ok=True)

    def update(self, state: dict):
        self.directory = state["directory"]
        self.bucket = state["bucket"]
        self.capacity = state["capacity"]
        self.size = state["size"]
        self.partition_no = state["partition_no"]
        self.properties = state["properties"]

    def data(self) -> dict:
        return {
            "directory": str(self.directory),
            "bucket": self.bucket,
            "capacity": self.capacity,
            "size": self.size,
            "partition_no": self.partition_no,
            "properties": self.properties,
        }

    def load(self, partition) -> None:
        self.clear()
        with open(partition, "r") as f:
            self.update(json.load(f))

    def dump(self) -> None:
        filepath = (
            Path(self.directory)
            .joinpath(*self.bucket)
            .joinpath(f"edge-property-partition-{self.partition_no}.json")
        )
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(self.data(), f)

    def partitions(self, buckets=None, reverse=False):
        if buckets:
            partitions = sorted(
                [*Path(self.directory).joinpath(*buckets).glob("*.json")],
                key=extract,
                reverse=True,
            )
        else:
            partitions = sorted(
                [*Path(self.directory).glob("./**/*.json")],
                key=extract,
                reverse=True,
            )
        return partitions

    def clear(self):
        self.bucket = []
        self.partition_no = 0
        self.size = 0
        self.properties = {}


class EdgeRelationPartition:
    __slots__ = [
        "directory",
        "capacity",
        "bucket",
        "size",
        "partition_no",
        "relations",
    ]

    def __init__(
        self,
        directory: str = ".",
        bucket: list[str] = [],
        capacity: int = 1000,
        partition_no: int = 0,
    ) -> None:
        self.directory = directory
        self.bucket = bucket
        self.capacity = capacity
        self.partition_no = partition_no
        self.size = 0
        self.relations = {"in": {}, "out": {}}

        Path(self.directory).mkdir(parents=True, exist_ok=True)

    def update(self, state: dict) -> None:
        self.directory = state["directory"]
        self.bucket = state["bucket"]
        self.capacity = state["capacity"]
        self.size = state["size"]
        self.partition_no = state["partition_no"]
        self.relations = state["relations"]
        unserialized_relations = {
            "in": {k: set(self.relations["in"][k]) for k in self.relations["in"]},
            "out": {k: set(self.relations["out"][k]) for k in self.relations["out"]},
        }
        self.relations = unserialized_relations

    def data(self) -> dict:
        serialized_relations = {
            "in": {k: list(self.relations["in"][k]) for k in self.relations["in"]},
            "out": {k: list(self.relations["out"][k]) for k in self.relations["out"]},
        }
        return {
            "directory": str(self.directory),
            "bucket": self.bucket,
            "capacity": self.capacity,
            "size": self.size,
            "partition_no": self.partition_no,
            "relations": serialized_relations,
        }

    def load(self, partition):
        self.clear()
        with open(partition, "r") as f:
            self.update(json.load(f))

    def dump(self):
        filepath = (
            Path(self.directory)
            .joinpath(*self.bucket)
            .joinpath(f"edge-relation-partition-{self.partition_no}.json")
        )
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(self.data(), f)

    def partitions(self, buckets=None, reverse=False):
        if buckets:
            partitions = sorted(
                [*Path(self.directory).joinpath(*buckets).glob("*.json")],
                key=extract,
                reverse=True,
            )
        else:
            partitions = sorted(
                [*Path(self.directory).glob("./**/*.json")],
                key=extract,
                reverse=True,
            )
        return partitions

    def clear(self):
        self.bucket = []
        self.partition_no = 0
        self.size = 0
        self.relations = {"in": {}, "out": {}}


class NodePartitionManager(object):
    def __init__(
        self,
        edges,
        capacity: int,
        partition_bucket_depth: int = 1,
        directory: str = "./graph/nodes",
    ) -> None:
        self.capacity = capacity
        self.directory = Path(directory)
        self.partition_bucket_depth = partition_bucket_depth
        self.edges = edges
        self.data = {}

    def create(self, key, properties=None) -> bool:
        try:
            self.validate(key)
            can_create = not self.contains(key)
            if not can_create:
                return False

            buckets = []
            if self.partition_bucket_depth > 0:
                buckets = self.partition_id(key)

            np = self.get_partition_to_create_node(buckets=buckets)
            if np is None:
                args = {
                    "capacity": self.capacity,
                    "directory": self.directory,
                    "bucket": buckets,
                }
                np = NodePartition(**args)
                np.partition_no = len(np.partitions(buckets))
                np.nodes[key] = properties
                np.size += 1
                np.dump()
                return True
            if np.size < self.capacity:
                np = np
                np.nodes[key] = properties
                np.size += 1
                np.dump()
                return True
            else:
                np = np
                np.nodes[key] = properties
                np.size += 1
                if np.size == np.capacity:
                    np.dump()
                return True
        except Exception:
            traceback.print_exc()
        return False

    def get(self, key) -> Node:
        try:
            self.validate(key)
            buckets = self.partition_id(key)
            np = self.get_partition_to_contain_node(key, buckets=buckets)
            if np != None:
                node = Node(key, np.nodes[key])
                return node
            return None
        except Exception:
            traceback.print_exc()
        return None

    def update(self, key, properties) -> bool:
        try:
            self.validate(key)
            buckets = self.partition_id(key)
            np = self.get_partition_to_contain_node(key, buckets=buckets)
            if np != None:
                np.nodes[key] = properties
                np.dump()
                return True
            return False
        except Exception:
            traceback.print_exc()
        return False

    def delete(self, key) -> bool:
        try:
            self.validate(key)
            buckets = self.partition_id(key)
            np = self.get_partition_to_contain_node(key, buckets=buckets)
            deleted_node = False
            if np != None:
                del np.nodes[key]
                np.size -= 1
                np.dump()
                deleted_node = True

            result = [deleted_node, self.edges.delete_all(key)]
            return all(result)
        except Exception:
            traceback.print_exc()
        return False

    def contains(self, key) -> Node:
        try:
            self.validate(key)
            buckets = self.partition_id(key)
            np = self.get_partition_to_contain_node(key, buckets=buckets)
            # if np != None:
            #     return np != None or key in self.np.nodes
            return np != None
        except Exception:
            traceback.print_exc()
        return False

    def get_all(self) -> iter:
        try:
            args = {
                "capacity": self.capacity,
                "directory": self.directory,
            }
            np = NodePartition(**args)
            for p in np.partitions():
                np.load(p)
                for key in np.nodes:
                    n = Node(key, np.nodes[key])
                    yield n
        except Exception:
            traceback.print_exc()
        return []

    def get_keys(self) -> iter:
        try:
            args = {
                "capacity": self.capacity,
                "directory": self.directory,
            }
            np = NodePartition(**args)
            for p in np.partitions():
                np.load(p)
                for key in np.nodes:
                    yield key
        except Exception:
            traceback.print_exc()
        return []

    def validate(self, key):
        if not isinstance(key, str):
            raise TypeError("key is not an instance of str. Must be str.")

    def partition_id(self, key: str) -> list:
        result = []
        for i in range(min(len(key), self.partition_bucket_depth)):
            c = key[i]
            if c in ascii_lowercase:
                result.append(c)
            elif c in ascii_uppercase:
                result.append(c)
            elif c in digits:
                result.append(c)
            elif c in punctuation:
                result.append(".punctuation")
            else:
                result.append(".misc")
        return result

    def get_partition_to_contain_node(self, key, buckets: list = None):
        args = {
            "capacity": self.capacity,
            "directory": self.directory,
        }
        np = NodePartition(**args)
        partitions = np.partitions(buckets=buckets)
        for partition in partitions:
            np.load(partition)
            if key in np.nodes:
                return np

    def get_partition_to_create_node(self, buckets: list = None, reverse: bool = False):
        args = {
            "capacity": self.capacity,
            "directory": self.directory,
            "bucket": buckets,
        }
        np = NodePartition(**args)
        partitions = np.partitions(buckets=buckets, reverse=reverse)
        for partition in partitions:
            np.load(partition)
            if np.size < self.capacity:
                return np


class EdgePartitionManager(object):
    def __init__(
        self,
        capacity: int,
        partition_bucket_depth: int = 1,
        directory: str = "./graph/edges",
    ) -> None:
        self.capacity = capacity
        self.directory = Path(directory)
        self.partition_bucket_depth = partition_bucket_depth
        # args = {
        #     "directory": self.directory.joinpath("relations"),
        #     "bucket": [],
        #     "capacity": self.capacity,
        # }
        # self.erp: EdgeRelationPartition = EdgeRelationPartition(**args)

        # args = {
        #     "directory": self.directory.joinpath("properties"),
        #     "bucket": [],
        #     "capacity": self.capacity,
        # }
        # self.epp: EdgePropertyPartition = EdgePropertyPartition(**args)

    def create(self, source, target, properties=None) -> bool:
        try:
            self.validate(source)
            self.validate(target)
            buckets = []
            if self.partition_bucket_depth > 0:
                buckets = self.partition_id(source)

            erp, can_create_er = self.partition_for_create_edge_relation(
                source, target, buckets=buckets, check_existence=True
            )
            epp, can_create_ep = self.partition_for_create_edge_property(
                source, target, buckets=buckets, check_existence=True
            )
            if can_create_er == False or can_create_ep == False:
                return False

            if erp is None:
                args = {
                    "directory": self.directory.joinpath("relations"),
                    "bucket": buckets,
                    "capacity": self.capacity,
                }
                erp = EdgeRelationPartition(**args)
                erp.partition_no = erp.partition_no + 1
                erp.relations["out"].setdefault(source, set())
                erp.relations["in"].setdefault(target, set())
                erp.relations["out"][source].add(target)
                erp.relations["in"][target].add(source)
                erp.size += 1
                erp.dump()
            else:
                # if source in self.erp.relations["out"]:
                #     if target in self.erp.relations["out"][source]:
                #         return False
                erp.relations["out"].setdefault(source, set())
                erp.relations["in"].setdefault(target, set())
                erp.relations["out"][source].add(target)
                erp.relations["in"][target].add(source)
                erp.size += 1
                erp.dump()

            if epp is None:
                args = {
                    "directory": self.directory.joinpath("properties"),
                    "bucket": buckets,
                    "capacity": self.capacity,
                }
                epp = EdgePropertyPartition(**args)
                epp.partition_no = epp.partition_no + 1
                epp.properties.setdefault(source, {})
                epp.properties[source][target] = properties
                epp.size += 1
                epp.dump()
            else:
                epp.properties.setdefault(source, {})
                epp.properties[source][target] = properties
                epp.size += 1
                epp.dump()
            return True
        except Exception:
            traceback.print_exc()
        return False

    def get(self, source, target) -> Edge:
        # self.commit_epp()
        try:
            self.validate(source)
            self.validate(target)
            # print(source, target)
            buckets = []
            if self.partition_bucket_depth > 0:
                buckets = self.partition_id(source)
            args = {
                "directory": self.directory.joinpath("properties"),
                "bucket": buckets,
                "capacity": self.capacity,
            }
            epp = EdgePropertyPartition(**args)
            partitions = epp.partitions(buckets=buckets)
            for p in partitions:
                epp.load(p)
                if source in epp.properties:
                    if target in epp.properties[source]:
                        properties = epp.properties[source][target]
                        edge = Edge(source, target, properties)
                        return edge
            return None
        except Exception:
            traceback.print_exc()
        return None

    def get_incoming(self, key) -> iter:
        # self.commit()
        try:
            self.validate(key)
            args = {
                "directory": self.directory.joinpath("relations"),
                "capacity": self.capacity,
            }
            erp = EdgeRelationPartition(**args)
            for p in erp.partitions():
                erp.load(p)
                if key in erp.relations["in"]:
                    for source in erp.relations["in"][key]:
                        yield source
        except Exception:
            traceback.print_exc()
        return []

    def get_outgoing(self, key) -> iter:
        try:
            self.validate(key)
            args = {
                "directory": self.directory.joinpath("relations"),
                "capacity": self.capacity,
            }
            erp = EdgeRelationPartition(**args)
            for p in erp.partitions():
                erp.load(p)
                if key in erp.relations["out"]:
                    for source in erp.relations["out"][key]:
                        yield source
        except Exception:
            traceback.print_exc()
        return []

    def update(self, source, target, properties=None) -> bool:
        try:
            self.validate(source)
            self.validate(target)
            buckets = []
            if self.partition_bucket_depth > 0:
                buckets = self.partition_id(source)
            args = {
                "directory": self.directory.joinpath("properties"),
                "bucket": buckets,
                "capacity": self.capacity,
            }

            epp = EdgePropertyPartition(**args)

            partitions = epp.partitions(buckets=buckets)

            for i, p in enumerate(partitions):
                epp.load(p)
                if source in epp.properties:
                    if target in epp.properties[source]:
                        epp.properties[source][target] = properties
                        epp.dump()
                        return True
        except Exception:
            traceback.print_exc()
        return False

    def delete(self, source, target) -> bool:
        try:
            self.validate(source)
            self.validate(target)
            buckets = []
            if self.partition_bucket_depth > 0:
                buckets = self.partition_id(source)
            args = {
                "directory": self.directory.joinpath("relations"),
                "bucket": buckets,
                "capacity": self.capacity,
            }
            print(args)
            erp = EdgeRelationPartition(**args)
            erp_partitions = erp.partitions()
            edge_relationship_deleted = False
            for p in erp_partitions:
                erp.load(p)
                if source in erp.relations["out"]:
                    if target in erp.relations["out"][source]:
                        erp.relations["out"][source].remove(target)
                        erp.relations["in"][target].remove(source)

                        if source in erp.relations["out"]:
                            if len(erp.relations["out"][source]) == 0:
                                del erp.relations["out"][source]
                        if target in erp.relations["in"]:
                            if len(erp.relations["in"][target]) == 0:
                                del erp.relations["in"][target]

                        erp.size -= 1
                        edge_relationship_deleted = True
                        erp.dump()
                        break
            args = {
                "directory": self.directory.joinpath("properties"),
                "capacity": self.capacity,
            }
            epp = EdgePropertyPartition(**args)
            epp_partitions = epp.partitions()

            edge_value_deleted = False
            for p in epp_partitions:
                epp.load(p)
                if source in epp.properties:
                    if target in epp.properties[source]:
                        epp.size -= 1
                        del epp.properties[source][target]
                        if len(epp.properties[source]) == 0:
                            del epp.properties[source]
                        edge_value_deleted = True
                        epp.dump()
                        break
            print(edge_relationship_deleted, edge_value_deleted)
            return edge_relationship_deleted and edge_value_deleted
        except Exception:
            traceback.print_exc()
        return False

    def delete_all(self, key):
        try:
            self.validate(key)
            args = {
                "directory": self.directory.joinpath("relations"),
                "capacity": self.capacity,
            }
            erp = EdgeRelationPartition(**args)
            erp_partitions = erp.partitions()

            for p in erp_partitions:
                erp.load(p)
                if key in erp.relations["out"]:
                    for out_key in erp.relations["out"][key]:
                        erp.relations["in"][out_key].remove(key)
                        erp.size -= 1
                        if len(erp.relations["in"][out_key]) == 0:
                            del erp.relations["in"][out_key]
                    del erp.relations["out"][key]
                if key in erp.relations["in"]:
                    for in_key in erp.relations["in"][key]:
                        erp.relations["out"][in_key].remove(key)
                        erp.size -= 1
                        if len(erp.relations["out"][in_key]) == 0:
                            del erp.relations["out"][in_key]
                        args = {
                            "capacity": self.capacity,
                            "directory": self.directory,
                        }
                        epp = EdgePropertyPartition(**args)
                        epp_partitions = epp.partitions()
                        for p in epp_partitions:
                            epp.load(p)
                            if in_key in epp.properties:
                                if key in epp.properties[in_key]:
                                    epp.size -= 1
                                    del epp.properties[in_key][key]
                                    if len(epp.properties[in_key]) == 0:
                                        del epp.properties[in_key]
                                    epp.dump()
                                    break
                    del erp.relations["in"][key]
                erp.dump()

            args = {
                "capacity": self.capacity,
                "directory": self.directory.joinpath("properties"),
            }
            epp = EdgePropertyPartition(**args)
            epp_partitions = epp.partitions()

            for p in epp_partitions:
                epp.load(p)
                if key in epp.properties:
                    epp.size -= len(epp.properties[key])
                    del epp.properties[key]
                    epp.dump()

            return True
        except Exception:
            traceback.print_exc()
        return False

    def get_all(self) -> iter:
        try:
            args = {
                "capacity": self.capacity,
                "directory": self.directory.joinpath("properties"),
            }
            epp = EdgePropertyPartition(**args)
            epp_partitions = epp.partitions()
            for p in epp_partitions:
                epp.load(p)
                for source in epp.properties:
                    for target in epp.properties[source]:
                        properties = epp.properties[source][target]
                        edge = Edge(source, target, properties)
                        yield edge
        except Exception:
            traceback.print_exc()

    def contains(self, source, target) -> bool:
        try:
            self.validate(source)
            self.validate(target)
            buckets = []
            if self.partition_bucket_depth > 0:
                buckets = self.partition_id(source)
            erp = self.__parallel_exist_relation(source, target, buckets=buckets)
            return erp != None
        except Exception:
            traceback.print_exc()
        return False

    def validate(self, key):
        if not isinstance(key, str):
            raise TypeError("key is not an instance of str")

    def partition_id(self, key: str) -> list:
        result = []
        for i in range(min(len(key), self.partition_bucket_depth)):
            c = key[i]
            if c in ascii_lowercase:
                result.append(c)
            elif c in ascii_uppercase:
                result.append(c)
            elif c in digits:
                result.append(c)
            elif c in punctuation:
                result.append(".punctuation")
            else:
                result.append(".misc")
        return result

    def __parallel_exist_property(self, source, target, buckets: list = None):
        args = {
            "directory": self.directory.joinpath("properties"),
            "bucket": buckets,
            "capacity": self.capacity,
        }
        epp = EdgePropertyPartition(**args)
        partitions = epp.partitions(buckets=buckets)
        for partition in partitions:
            epp.load(partition)
            if source in epp.properties:
                if target in epp.properties[source]:
                    return epp

    def __parallel_exist_relation(self, source, target, buckets: list = None):
        # if source in self.erp.relations["out"]:
        #     if target in self.erp.relations["out"][source]:
        #         return self.erp
        args = {
            "directory": self.directory.joinpath("relations"),
            "bucket": buckets,
            "capacity": self.capacity,
        }
        erp = EdgeRelationPartition(**args)
        partitions = erp.partitions(buckets=buckets)
        for partition in partitions:
            erp.load(partition)
            if source in erp.relations["out"]:
                if target in erp.relations["out"][source]:
                    return erp

    def partition_for_create_edge_relation(
        self,
        source,
        target,
        buckets: list = [],
        reverse: bool = True,
        check_existence: bool = True,
    ):
        # if buckets == self.erp.bucket:
        #     if self.erp.size < self.capacity:
        #         return self.erp, True
        args = {
            "directory": self.directory.joinpath("relations"),
            "bucket": buckets,
            "capacity": self.capacity,
        }
        erp = EdgeRelationPartition(**args)
        partitions = erp.partitions(buckets=buckets, reverse=reverse)
        available_partition = None
        for partition in partitions:
            erp.load(partition)
            if check_existence:
                if source in erp.relations["out"]:
                    if target in erp.relations["out"][source]:
                        return None, False
            if erp.size < self.capacity:
                if available_partition == None:
                    available_partition = erp
                else:
                    continue
        return available_partition, True

    def partition_for_create_edge_property(
        self,
        source,
        target,
        buckets: list = None,
        reverse: bool = False,
        check_existence: bool = True,
    ):
        args = {
            "directory": self.directory.joinpath("properties"),
            "bucket": buckets,
            "capacity": self.capacity,
        }
        epp = EdgePropertyPartition(**args)
        partitions = epp.partitions(buckets=buckets, reverse=reverse)
        available_partition = None
        for partition in partitions:
            epp.load(partition)
            if check_existence:
                if source in epp.properties:
                    if target in epp.properties[source]:
                        return None, False
            if epp.size < self.capacity:
                if available_partition == None:
                    available_partition = epp
                else:
                    continue
        return available_partition, True


class GRT(object):
    def __init__(
        self,
        node_partition_capacity=1000,
        edge_partition_capacity=1000,
        node_bucket_depth=1,
        edge_bucket_depth=1,
        directory="./graph",
    ) -> None:
        self.node_partition_capacity = node_partition_capacity
        self.edge_partition_capacity = edge_partition_capacity
        self.node_bucket_depth = node_bucket_depth
        self.edge_bucket_depth = edge_bucket_depth

        self.directory = Path(directory)
        self.directory_nodes = self.directory.joinpath("nodes")
        self.directory_edges = self.directory.joinpath("edges")

        self.directory.mkdir(parents=True, exist_ok=True)
        self.directory_nodes.mkdir(parents=True, exist_ok=True)
        self.directory_edges.mkdir(parents=True, exist_ok=True)

        edges_kwargs = {
            "capacity": self.edge_partition_capacity,
            "directory": self.directory_edges,
            "partition_bucket_depth": self.edge_bucket_depth,
        }
        self.edges = EdgePartitionManager(**edges_kwargs)

        nodes_kwargs = {
            "edges": self.edges,
            "capacity": self.node_partition_capacity,
            "directory": self.directory_nodes,
            "partition_bucket_depth": self.node_bucket_depth,
        }
        self.nodes = NodePartitionManager(**nodes_kwargs)

    def clear(self):
        try:
            shutil.rmtree(self.directory)
            self.directory.mkdir(parents=True, exist_ok=True)
            self.directory_nodes.mkdir(parents=True, exist_ok=True)
            self.directory_edges.mkdir(parents=True, exist_ok=True)
            return True
        except Exception:
            traceback.print_exc()
        return False

    def copy(self, graph):
        try:
            shutil.copytree(graph.directory, self.directory, dirs_exist_ok=True)
            NODES_DIR = Path(self.directory.joinpath("nodes"))
            for file_path in NODES_DIR.glob("./**/*.json"):
                with open(file_path, "r") as f:
                    data = json.load(f)
                    data["directory"] = str(NODES_DIR)
                with open(file_path, "w") as f:
                    f.write(json.dumps(data))
            EDGES_DIR = Path(self.directory.joinpath("edges"))
            for file_path in EDGES_DIR.glob("./**/*.json"):
                with open(file_path, "r") as f:
                    data = json.load(f)
                    data["directory"] = str(EDGES_DIR)
                with open(file_path, "w") as f:
                    f.write(json.dumps(data))
            return True
        except Exception:
            traceback.print_exc()
        return False
