import random
import numpy as np

def generate_graph_dataset(max_node_id=4096, min_degree=10, max_degree=96, node_probability=0.1, output_file="data.txt"):
    """
    Generate a graph dataset with the following properties:
    - Node IDs range from 0 to max_node_id
    - Only some nodes are active in the graph (controlled by node_probability)
    - Active nodes have between min_degree and max_degree connections
    - Outputs in format: FromNodeId\tToNodeId
    """
    # Initialize structures
    edges = set()  # Use a set to avoid duplicate edges
    degrees = np.zeros(max_node_id + 1, dtype=int)

    # Decide which nodes will be active in the graph
    active_nodes = []
    for node_id in range(max_node_id + 1):
        if random.random() > node_probability:
            active_nodes.append(node_id)

    print(f"Selected {len(active_nodes)} active nodes out of {max_node_id + 1} possible nodes")

    # For each active node, generate connections
    for node_id in active_nodes:
        # Target degree for this node
        target_degree = random.randint(min_degree, max_degree)

        # Add edges until we reach the target degree
        attempts = 0
        while degrees[node_id] < target_degree and attempts < 3 * target_degree:
            # Choose a random target node from active nodes (not itself)
            target_node = random.choice(active_nodes)
            while target_node == node_id:
                target_node = random.choice(active_nodes)

            # Add edge if it doesn't exceed target degree of the target node
            if degrees[target_node] < max_degree:
                edge = (node_id, target_node)

                # Only add if not already present
                if edge not in edges:
                    edges.add(edge)
                    degrees[node_id] += 1
                    degrees[target_node] += 1

            attempts += 1

    # Convert set to list and sort for consistency
    edge_list = sorted(list(edges))

    # Write to file
    with open(output_file, "w") as f:
        f.write("# FromNodeId\tToNodeId\n")
        for src, dst in edge_list:
            f.write(f"{src}\t{dst}\n")

    # Calculate active node degrees (only for nodes that have at least one connection)
    active_degrees = degrees[degrees > 0]
    print(f"Created graph with {len(active_degrees)} connected nodes and {len(edges)} edges")
    if len(active_degrees) > 0:
        print(f"Among connected nodes: Min degree: {min(active_degrees)}, Max degree: {max(active_degrees)}")

    # Print some statistics for random active nodes
    for i in range(5):
        if active_nodes:
            node = random.choice(active_nodes)
            print(f"Node {node} has {degrees[node]} connections")

if __name__ == "__main__":
    generate_graph_dataset()