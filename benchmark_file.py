from tqdm import tqdm
from grt_file import GRT
import os
import shutil

NUM_NODES = 10000
NUM_EDGES = 10000

# Define the benchmark directory for the file-based graph database.
benchmark_dir = "./database_benchmark_file"

# Clear benchmark directory if it already exists.
if os.path.exists(benchmark_dir):
    shutil.rmtree(benchmark_dir)

# Initialize the graph with the specified directory.
graph = GRT(directory=benchmark_dir)

# 1. Create Nodes
print("Creating Nodes...")
for i in tqdm(range(NUM_NODES)):
    graph.nodes.create(key=f"node_{i}", properties=f"props_{i}")

# 2. Create Edges
print("Creating Edges...")
for i in tqdm(range(NUM_EDGES)):
    src = f"node_{i % NUM_NODES}"
    dest = f"node_{(i + 1) % NUM_NODES}"
    graph.edges.create(src, dest, properties=f"edge_props_{i}")

# 3. Query All Nodes
print("Querying All Nodes...")
all_nodes = graph.nodes.all()  # returns a list of node keys

# 4. Query All Edges
print("Querying All Edges...")
all_edges = graph.edges.all()  # returns a list of (source, destination) tuples

# 5. Update Nodes
print("Updating Nodes...")
for i in tqdm(range(NUM_NODES)):
    graph.nodes.update(key=f"node_{i}", properties=f"updated_props_{i}")

# 6. Delete Nodes (which also deletes associated edges)
print("Deleting Nodes...")
for i in tqdm(range(NUM_NODES)):
    graph.nodes.delete(key=f"node_{i}")

print("Benchmarking Completed!")
