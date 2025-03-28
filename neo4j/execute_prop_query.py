from neo4j import GraphDatabase
import time
import os
from neo4j_auth import URI, USERNAME, PASSWORD

def run_query_and_save_results():
    # Connect to Neo4j
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

    # Read input query from file
    with open("../input_query.txt", "r") as f:
        input_text = f.read().strip()

    # Convert string representation of list to actual list
    input_text = input_text.strip('[]')
    input_values = [int(x.strip()) for x in input_text.split(',') if x.strip()]

    # Create a session
    session = driver.session()

    # Start timing
    start_time = time.time()

    # BFS propagation results
    all_levels = []

    # Level 0: Starting nodes (input values)
    level0_query = """
    MATCH (n:Product)
    WHERE n.id IN $input_values
    RETURN collect(n.id) AS nodes
    """
    level0_result = session.run(level0_query, input_values=input_values)
    level0_nodes = level0_result.single()["nodes"]
    all_levels.append(level0_nodes)

    # Level 1: Direct neighbors of starting nodes
    level1_query = """
    MATCH (n:Product)-[:CO_PURCHASED]->(neighbor:Product)
    WHERE n.id IN $prev_level_nodes
    RETURN collect(DISTINCT neighbor.id) AS nodes
    """
    level1_result = session.run(level1_query, prev_level_nodes=level0_nodes)
    level1_nodes = level1_result.single()["nodes"]
    all_levels.append(level1_nodes)

    # Level 2: Neighbors of level 1 nodes
    level2_query = """
    MATCH (n:Product)-[:CO_PURCHASED]->(neighbor:Product)
    WHERE n.id IN $prev_level_nodes
    RETURN collect(DISTINCT neighbor.id) AS nodes
    """
    level2_result = session.run(level2_query, prev_level_nodes=level1_nodes)
    level2_nodes = level2_result.single()["nodes"]
    all_levels.append(level2_nodes)

    # Level 3: Neighbors of level 2 nodes
    level3_query = """
    MATCH (n:Product)-[:CO_PURCHASED]->(neighbor:Product)
    WHERE n.id IN $prev_level_nodes
    RETURN collect(DISTINCT neighbor.id) AS nodes
    """
    level3_result = session.run(level3_query, prev_level_nodes=level2_nodes)
    level3_nodes = level3_result.single()["nodes"]
    all_levels.append(level3_nodes)

    # Level 4: Neighbors of level 3 nodes
    level4_query = """
    MATCH (n:Product)-[:CO_PURCHASED]->(neighbor:Product)
    WHERE n.id IN $prev_level_nodes
    RETURN collect(DISTINCT neighbor.id) AS nodes
    """
    level4_result = session.run(level4_query, prev_level_nodes=level3_nodes)
    level4_nodes = level4_result.single()["nodes"]
    all_levels.append(level4_nodes)

    # End timing
    end_time = time.time()
    execution_time = end_time - start_time

    # Close the session
    session.close()
    driver.close()

    # Write results to file
    with open("Neo4J_results.txt", "w") as f:
        for level_nodes in all_levels:
            f.write(str(level_nodes) + "\n")

    # Write stats to file
    with open("Neo4J_stats.txt", "w") as f:
        f.write(f"Execution time: {execution_time:.6f} seconds\n")
        f.write(f"Number of levels: {len(all_levels)}\n")
        for i, level in enumerate(all_levels):
            f.write(f"Level {i} nodes: {len(level)}\n")

    print(f"Query executed in {execution_time:.6f} seconds")
    print(f"Results saved to Neo4J_results.txt")
    print(f"Stats saved to Neo4J_stats.txt")

if __name__ == "__main__":
    run_query_and_save_results()