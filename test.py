import pytest
from grt import GRT

@pytest.fixture
def graph(tmp_path):
    # Use a temporary directory for each test run to isolate file I/O.
    test_dir = tmp_path / "test.graph"
    grt = GRT(directory=str(test_dir))
    yield grt
    # Cleanup is automatic with tmp_path; if needed, you could add:
    # shutil.rmtree(test_dir, ignore_errors=True)

# Test Node Creation
def test_create_node(graph):
    graph.nodes.create("node1", {"property": "value"})
    # Check that the node file exists and its content is correct.
    assert graph.nodes.contains("node1")
    node = graph.nodes.get("node1")
    assert node == {"property": "value"}

# Test Node Retrieval
def test_get_node(graph):
    graph.nodes.create("node1", {"property": "value"})
    node = graph.nodes.get("node1")
    assert node is not None
    assert node.get("property") == "value"

# Test Node Update
def test_update_node(graph):
    graph.nodes.create("node1", {"property": "value"})
    graph.nodes.update("node1", {"property": "new_value"})
    node = graph.nodes.get("node1")
    assert node.get("property") == "new_value"

# Test Node Deletion
def test_delete_node(graph):
    graph.nodes.create("node1", {"property": "value"})
    graph.nodes.delete("node1")
    node = graph.nodes.get("node1")
    assert node is None

# Test Edge Creation
def test_create_edge(graph):
    # Create nodes first as prerequisites
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    assert graph.edges.contains("node1", "node2")
    edge = graph.edges.get("node1", "node2")
    assert edge == {"weight": 5}

# Test Edge Retrieval
def test_get_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    edge = graph.edges.get("node1", "node2")
    assert edge is not None
    assert edge.get("weight") == 5

# Test Edge Update
def test_update_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    graph.edges.update("node1", "node2", {"weight": 10})
    edge = graph.edges.get("node1", "node2")
    assert edge.get("weight") == 10

# Test Edge Deletion
def test_delete_edge(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    graph.edges.delete("node1", "node2")
    edge = graph.edges.get("node1", "node2")
    assert edge is None

# Test Retrieving All Nodes
def test_get_all_nodes(graph):
    graph.nodes.create("node1", {"property": "value"})
    graph.nodes.create("node2", {"property": "value2"})
    nodes = graph.nodes.all()
    assert len([_ for _ in nodes]) == 2

# Test Retrieving All Edges
def test_get_all_edges(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    edges = graph.edges.all()
    # edges returns an iter of tuples (src, dest)
    assert len([_ for _ in edges]) == 1

# Test Incoming and Outgoing Edge Retrieval
def test_incoming_outgoing_edges(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.nodes.create("node3")
    # Create multiple edges with node3 as the target for two nodes,
    # and one edge with node1 as the source.
    graph.edges.create("node1", "node3", {"relation": "a"})
    graph.edges.create("node2", "node3", {"relation": "b"})
    graph.edges.create("node3", "node1", {"relation": "c"})
    incoming_to_3 = graph.edges.incoming("node3")
    outgoing_from_1 = graph.edges.outgoing("node1")
    # incoming should list the source nodes that point to node3.
    assert set(incoming_to_3) == {"node1", "node2"}
    # outgoing from node1 should include node3.
    assert [_ for _ in outgoing_from_1] == ["node3"]

# Test that Deleting a Node Also Deletes Related Edges
def test_delete_node_deletes_edges(graph):
    graph.nodes.create("node1")
    graph.nodes.create("node2")
    graph.edges.create("node1", "node2", {"weight": 5})
    # Delete node1 should remove its outgoing edge.
    graph.nodes.delete("node1")
    assert not graph.nodes.contains("node1")
    edge = graph.edges.get("node1", "node2")
    assert edge is None
