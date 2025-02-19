import pytest
from grt_sqlite import GRT

@pytest.fixture
def graph():
    g = GRT("./database_test")
    yield g
    g.clear()
    g.close()

# Test Node Creation
def test_create_node(graph):
    result = graph.nodes.create("node1", "{'property': 'value'}")
    assert result is True

def test_get_node(graph):
    graph.nodes.create("node1", "{'property': 'value'}")
    node = graph.nodes.get("node1")
    assert node.key == "node1"
    assert node.properties == "{'property': 'value'}"

def test_update_node(graph):
    graph.nodes.create("node1", "{'property': 'value'}")
    result = graph.nodes.update("node1", "{'property': 'new_value'}")
    assert result is True
    node = graph.nodes.get("node1")
    assert node.properties == "{'property': 'new_value'}"

def test_delete_node(graph):
    graph.nodes.create("node1")
    result = graph.nodes.delete("node1")
    assert result is False
    node = graph.nodes.get("node1")
    assert node is None

# Test Edge Creation
def test_create_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    result = graph.edges.create("node1", "node2", "{'weight': 5}")
    assert result is True

def test_get_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", "{'weight': 5}")
    edge = graph.edges.get("node1", "node2")
    assert edge.source == "node1"
    assert edge.target == "node2"
    assert edge.properties == "{'weight': 5}"

def test_update_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", "{'weight': 5}")
    result = graph.edges.update("node1", "node2", "{'weight': 10}")
    assert result is True
    edge = graph.edges.get("node1", "node2")
    assert edge.properties == "{'weight': 10}"

def test_delete_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2")
    result = graph.edges.delete("node1", "node2")
    assert result is True
    edge = graph.edges.get("node1", "node2")
    assert edge is None

# Test Get All Nodes and Edges
def test_get_all_nodes(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    nodes = list(graph.nodes.get_all())
    assert len(nodes) == 2

def test_get_all_edges(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2")
    graph.edges.create("node2", "node1")
    edges = list(graph.edges.get_all())
    assert len(edges) == 2
