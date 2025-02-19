# Authors:
# - Zakar Handricken
import shutil
import traceback
from pathlib import Path
import sqlite3

extract = lambda x: int(str(x).split("-")[-1].split(".")[0])


class SQL:
    # TABLES
    @staticmethod
    def create_table_nodes_sql() -> str:
        return 'CREATE TABLE IF NOT EXISTS "nodes" ("key" TEXT NOT NULL, "properties" TEXT, PRIMARY KEY("key"));'

    @staticmethod
    def create_table_edges_sql() -> str:
        return 'CREATE TABLE IF NOT EXISTS "edges" ("source" TEXT, "target" TEXT, "properties" TEXT, UNIQUE("source", "target"));'

    @staticmethod
    def delete_table_nodes_sql() -> str:
        return "DROP TABLE nodes;"

    @staticmethod
    def delete_table_edges_sql() -> str:
        return "DROP TABLE edges;"

    # NODES
    @staticmethod
    def create_node_sql() -> str:
        return 'INSERT INTO nodes ("key", "properties") VALUES (?, ?);'

    @staticmethod
    def create_node_without_properties_sql() -> str:
        return 'INSERT INTO nodes ("key") VALUES (?);'

    @staticmethod
    def get_node_sql() -> str:
        return 'SELECT * FROM "main"."nodes" WHERE "key"=(?);'

    @staticmethod
    def update_node_sql() -> str:
        return 'UPDATE "main"."nodes" SET "properties"=(?) WHERE "key"=(?);'

    @staticmethod
    def delete_node_sql() -> str:
        return 'DELETE FROM "main"."nodes" WHERE "key"=(?);'

    @staticmethod
    def contains_node_sql() -> str:
        return 'SELECT * FROM "main"."nodes" WHERE "key"=(?) LIMIT 1;'

    @staticmethod
    def get_all_nodes_sql() -> str:
        return 'SELECT * FROM "main"."nodes";'

    @staticmethod
    def get_all_node_keys_sql() -> str:
        return 'SELECT "key" FROM "main"."nodes";'

    # EDGES
    @staticmethod
    def create_edge_sql() -> str:
        return 'INSERT INTO "main"."edges" ("source", "target", "properties") VALUES (?, ?, ?);'

    @staticmethod
    def create_edge_without_properties_sql() -> str:
        return 'INSERT INTO "main"."edges" ("source", "target") VALUES (?, ?);'

    @staticmethod
    def get_edge_sql() -> str:
        return 'SELECT * FROM "main"."edges" WHERE "source"=(?) AND "target"=(?);'

    @staticmethod
    def get_edge_incoming_sql() -> str:
        return 'SELECT * FROM "main"."edges" WHERE "target"=(?);'

    @staticmethod
    def get_edge_outgoing_sql() -> str:
        return 'SELECT * FROM "main"."edges" WHERE "source"=(?);'

    @staticmethod
    def update_edge_sql() -> str:
        return 'UPDATE "main"."edges" SET "properties"=(?) WHERE "source"=(?) AND "target"=(?);'

    @staticmethod
    def delete_edge_sql() -> str:
        return 'DELETE FROM "main"."edges" WHERE "source"=(?) AND "target"=(?);'

    @staticmethod
    def delete_edge_incoming_sql() -> str:
        return 'DELETE FROM "main"."edges" WHERE "target"=(?);'

    @staticmethod
    def delete_edge_outgoing_sql() -> str:
        return 'DELETE FROM "main"."edges" WHERE "source"=(?);'

    @staticmethod
    def contains_edge_sql() -> str:
        return (
            'SELECT * FROM "main"."edges" WHERE "source"=(?) AND "target"=(?) LIMIT 1;'
        )

    @staticmethod
    def get_all_edges_sql() -> str:
        return 'SELECT * FROM "main"."edges";'

    @staticmethod
    def get_all_edge_keys_sql() -> str:
        return 'SELECT "source", "target" FROM "main"."edges";'


class Database(SQL):
    def __init__(self, directory: str) -> None:
        self.directory = directory
        self.connection = sqlite3.connect(directory)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def clear_data(self):
        self.cursor.execute(self.clear_data_sql())

    # TABLES
    def create_table_nodes(self):
        self.cursor.execute(self.create_table_nodes_sql())

    def create_table_edges(self):
        self.cursor.execute(self.create_table_edges_sql())

    def delete_table_nodes(self):
        self.cursor.execute(self.delete_table_nodes_sql())

    def delete_table_edges(self):
        self.cursor.execute(self.delete_table_edges_sql())

    # NODES
    def create_node(self, key: str, properties=None):
        with self.connection:
            self.cursor.execute(self.create_node_sql(), (key, properties))

    def get_node(self, key: str):
        return self.cursor.execute(self.get_node_sql(), (key,)).fetchone()

    def update_node(self, key: str, properties: None):
        with self.connection:
            self.cursor.execute(self.update_node_sql(), (properties, key))

    def delete_node(self, key: str):
        with self.connection:
            self.cursor.execute(self.delete_node_sql(), (key,))

    def get_all_nodes(self):
        return self.cursor.execute(self.get_all_nodes_sql(), ()).fetchall()

    def get_all_node_keys(self):
        return self.cursor.execute(self.get_all_node_keys_sql()).fetchall()

    # EDGES
    def create_edge(self, source: str, target: str, properties=None):
        with self.connection:
            self.cursor.execute(self.create_edge_sql(), (source, target, properties))

    def get_edge(self, source: str, target: str):
        return self.cursor.execute(self.get_edge_sql(), (source, target)).fetchone()

    def get_edge_incoming(self, target: str):
        return self.cursor.execute(self.get_edge_incoming_sql(), (target,)).fetchall()

    def get_edge_outgoing(self, source: str):
        return self.cursor.execute(self.get_edge_outgoing_sql(), (source,)).fetchall()

    def update_edge(self, source: str, target: str, properties=None):
        with self.connection:
            self.cursor.execute(self.update_edge_sql(), (properties, source, target))

    def delete_edge(self, source: str, target: str):
        with self.connection:
            self.cursor.execute(self.delete_edge_sql(), (source, target))

    def delete_edge_incoming(self, target: str):
        with self.connection:
            self.cursor.execute(self.delete_edge_incoming_sql(), (target,))

    def delete_edge_outgoing(self, source: str):
        with self.connection:
            self.cursor.execute(self.delete_edge_outgoing_sql(), (source,))

    def get_all_edges(self):
        return self.cursor.execute(self.get_all_edges_sql()).fetchall()


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


class NodeManager:
    def __init__(self, database: Database) -> None:
        self.database = database

    def create(self, key, properties=None) -> bool:
        try:
            self.database.create_node(key=key, properties=properties)
            return True
        except Exception:
            traceback.print_exc()
        return False

    def get(self, key) -> Node:
        try:
            data = self.database.get_node(key=key)
            if data is None:
                return None
            key, properties = data
            return Node(key=key, properties=properties)
        except Exception:
            traceback.print_exc()
        return None

    def update(self, key, properties) -> bool:
        try:
            self.database.update_node(key=key, properties=properties)
            return True
        except Exception:
            traceback.print_exc()
        return False

    def delete(self, key) -> bool:
        try:
            self.database.delete_node(key=key)
            self.database.delete_edge_outgoing(source=key)
            self.database.delete_edge_incoming(target=key)
        except Exception:
            traceback.print_exc()
        return False

    def get_all(self) -> iter:
        try:
            for data in self.database.get_all_nodes():
                yield Node(*data)
        except Exception:
            traceback.print_exc()
        return []

    def validate(self, key):
        if not isinstance(key, str):
            raise TypeError("key is not an instance of str. Must be str.")


class EdgeManager(object):
    def __init__(self, database: Database) -> None:
        self.database = database

    def create(self, source, target, properties=None) -> bool:
        try:
            self.database.create_edge(
                source=source, target=target, properties=properties
            )
            return True
        except Exception:
            traceback.print_exc()
        return False

    def get(self, source, target) -> Edge:
        try:
            data = self.database.get_edge(source=source, target=target)
            if data is None:
                return None
            source, target, properties = data
            return Edge(source, target, properties)
        except Exception:
            traceback.print_exc()
        return None

    def get_incoming(self, key) -> iter:
        try:
            for data in self.database.get_edge_incoming(target=key):
                source, target, properties = data
                yield Edge(source=source, target=target, properties=properties)
        except Exception:
            traceback.print_exc()
        return []

    def get_outgoing(self, key) -> iter:
        try:
            for data in self.database.get_edge_outgoing(source=key):
                yield Edge(*data)
        except Exception:
            traceback.print_exc()
        return []

    def update(self, source, target, properties=None) -> bool:
        try:
            self.validate(source)
            self.validate(target)
            self.database.update_edge(
                source=source, target=target, properties=properties
            )
            return True
        except Exception:
            traceback.print_exc()
        return False

    def delete(self, source, target) -> bool:
        try:
            self.validate(source)
            self.validate(target)
            self.database.delete_edge(source=source, target=target)
            return True
        except Exception:
            traceback.print_exc()
        return False

    def delete_all(self, key):
        try:
            self.validate(key)
            self.database.delete_edge_incoming(target=key)
            self.database.delete_edge_outgoing(source=key)
            return True
        except Exception:
            traceback.print_exc()
        return False

    def get_all(self) -> iter:
        try:
            for data in self.database.get_all_edges():
                yield Edge(*data)
        except Exception:
            traceback.print_exc()
        return []

    def validate(self, key):
        if not isinstance(key, str):
            raise TypeError("key is not an instance of str")


class GRT(object):
    def __init__(
        self,
        directory="./database",
    ) -> None:

        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

        self.directory_database = self.directory.joinpath("graph.db")

        self.database = Database(self.directory_database)
        self.database.create_table_nodes()
        self.database.create_table_edges()

        self.nodes = NodeManager(self.database)
        self.edges = EdgeManager(self.database)

    def close(self):
        self.database.connection.close()

    def clear(self):
        try:
            self.database = Database(self.directory_database)
            self.database.delete_table_edges()
            self.database.delete_table_nodes()
            self.database.create_table_nodes()
            self.database.create_table_edges()
        except Exception:
            traceback.print_exc()

    def copy(self, directory):
        try:
            shutil.copytree(self.directory, directory, dirs_exist_ok=True)
        except Exception:
            traceback.print_exc()
