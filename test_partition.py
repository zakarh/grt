import pytest
from grt_partition import GRT

@pytest.fixture
def graph(tmp_path):
    # Create a temporary directory for each test run
    test_dir = tmp_path / "graph_test"
    g = GRT(directory=str(test_dir))
    yield g
    g.clear()

# Test Node Creation
def test_create_node(graph):
    result = graph.nodes.create("node1", {"property": "value"})
    assert result is True
    node = graph.nodes.get("node1")
    assert node is not None
    assert node.key == "node1"
    assert node.properties == {"property": "value"}

# Test Node Retrieval
def test_get_node(graph):
    graph.nodes.create("node1", {"property": "value"})
    node = graph.nodes.get("node1")
    assert node is not None
    assert node.key == "node1"
    assert node.properties == {"property": "value"}

# Test Node Update
def test_update_node(graph):
    graph.nodes.create("node1", {"property": "value"})
    result = graph.nodes.update("node1", {"property": "new_value"})
    assert result is True
    node = graph.nodes.get("node1")
    assert node.properties == {"property": "new_value"}

# Test Node Deletion
def test_delete_node(graph):
    graph.nodes.create("node1", {"property": "value"})
    result = graph.nodes.delete("node1")
    # Expect deletion to succeed
    assert result is True
    node = graph.nodes.get("node1")
    assert node is None

# Test Edge Creation
def test_create_edge(graph):
    # Create prerequisite nodes
    graph.nodes.create("node1", {"property": "value"})
    graph.nodes.create("node2", {"property": "value2"})
    result = graph.edges.create("node1", "node2", {"weight": 5})
    assert result is True
    edge = graph.edges.get("node1", "node2")
    assert edge is not None
    assert edge.source == "node1"
    assert edge.target == "node2"
    assert edge.properties == {"weight": 5}

# Test Edge Retrieval
def test_get_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    edge = graph.edges.get("node1", "node2")
    assert edge is not None
    assert edge.source == "node1"
    assert edge.target == "node2"
    assert edge.properties == {"weight": 5}

# Test Edge Update
def test_update_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    result = graph.edges.update("node1", "node2", {"weight": 10})
    assert result is True
    edge = graph.edges.get("node1", "node2")
    assert edge.properties == {"weight": 10}

# Test Edge Deletion
def test_delete_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    assert graph.edges.contains("node1", "node2") # Remove
    result = graph.edges.delete("node1", "node2")
    assert result is True
    edge = graph.edges.get("node1", "node2")
    assert edge is None

# Test Retrieving All Nodes
def test_get_all_nodes(graph):
    graph.nodes.create("node1", {"property": "value"})
    graph.nodes.create("node2", {"property": "value2"})
    nodes = list(graph.nodes.get_all())
    keys = [node.key for node in nodes]
    assert "node1" in keys
    assert "node2" in keys
    assert len(keys) == 2

# Test Retrieving All Edges
def test_get_all_edges(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    edges = list(graph.edges.get_all())
    assert len(edges) == 1
    edge = edges[0]
    assert edge.source == "node1"
    assert edge.target == "node2"

# Test Incoming and Outgoing Edges
def test_incoming_outgoing_edges(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.nodes.create("node3")
    # Create multiple edges:
    graph.edges.create("node1", "node3", {"relation": "a"})
    graph.edges.create("node2", "node3", {"relation": "b"})
    graph.edges.create("node3", "node1", {"relation": "c"})
    incoming_to_3 = list(graph.edges.get_incoming("node3"))
    outgoing_from_1 = list(graph.edges.get_outgoing("node1"))
    # For node3, the incoming edges should come from node1 and node2.
    assert set(incoming_to_3) == {"node1", "node2"}
    # Outgoing from node1 should be node3.
    assert outgoing_from_1 == ["node3"]

# Test that Deleting a Node Also Deletes Its Related Edges
def test_delete_node_deletes_edges(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    result = graph.nodes.delete("node1")
    assert result is True
    assert graph.nodes.get("node1") is None
    edge = graph.edges.get("node1", "node2")
    assert edge is None
