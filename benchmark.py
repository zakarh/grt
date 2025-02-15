from tqdm import tqdm
from grt import GRT

NUM_NODES = 10000
NUM_EDGES = 10000

graph = GRT(directory="./database_benchmark")
graph.clear()

# 1. Create Nodes
print("Creating Nodes...")
for i in tqdm(range(NUM_NODES)):
    graph.nodes.create(key=f"node_{i}", properties=f"props_{i}")

# 2. Create Edges
print("Creating Edges...")
for i in tqdm(range(NUM_EDGES)):
    graph.edges.create(source=f"node_{i % NUM_NODES}", target=f"node_{(i + 1) % NUM_NODES}", properties=f"edge_props_{i}")

# 3. Query All Nodes
print("Querying All Nodes...")
all_nodes = [x for x in tqdm(graph.nodes.get_all())]

# 4. Query All Edges
print("Querying All Edges...")
all_edges = [x for x in tqdm(graph.edges.get_all())]

# 5. Update Nodes
print("Updating Nodes...")
for i in tqdm(range(NUM_NODES)):
    graph.nodes.update(key=f"node_{i}", properties=f"updated_props_{i}")

# 6. Delete Nodes and their edges
print("Deleting Nodes...")
for i in tqdm(range(NUM_NODES)):
    graph.nodes.delete(key=f"node_{i}")

graph.close()
print("Benchmarking Completed!")
