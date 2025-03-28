from neo4j import GraphDatabase
import time
import os
from neo4j_auth import URI, USERNAME, PASSWORD

# Setting for disabling Neo4j query optimizations
DISABLE_OPTIMIZATIONS = {"cypher.forbid_shortestpath_common_nodes": True,
                         "cypher.min_replan_interval": "1000000s",
                         "cypher.forbid_cartesian_products": True}

def run_query_and_save_results():
    # Connect to Neo4j with optimization settings turned off
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

    # Create a session with optimization settings disabled
    session_config = {"database": "neo4j"}  # Use default database

    # Read input query from file
    with open("../input_query.txt", "r") as f:
        input_text = f.read().strip()

    # Convert string representation of list to actual list
    input_text = input_text.strip('[]')
    input_values = [int(x.strip()) for x in input_text.split(',') if x.strip()]

    # Create a session with optimization settings disabled
    session = driver.session(**session_config)

    # Disable Neo4j query optimizations
    for setting, value in DISABLE_OPTIMIZATIONS.items():
        try:
            session.run(f"CALL dbms.setConfigValue('{setting}', '{value}')")
        except Exception as e:
            print(f"Warning: Could not set {setting}: {e}")

    # Ensure queries run linearly with APOC path expander (fallback to regular queries if APOC not available)
    has_apoc = True
    try:
        apoc_check = session.run("CALL dbms.procedures() YIELD name WHERE name STARTS WITH 'apoc' RETURN count(*) > 0 as has_apoc")
        has_apoc = apoc_check.single()["has_apoc"]
    except:
        has_apoc = False
        print("Warning: APOC procedures not available. Falling back to regular Cypher queries.")

    # Initialize results and statistics
    all_levels = []
    query_times = []

    # Open files for writing
    results_file = open("Neo4J_results.txt", "w")
    stats_file = open("Neo4J_stats.txt", "w")

    # Level 0: Starting nodes (input values)
    level0_nodes = input_values
    all_levels.append(sorted(level0_nodes))
    results_file.write(str(sorted(level0_nodes)) + "\n")

    # Query 1: Single hop query
    start_time = time.time() * 1000000  # Convert to microseconds

    if has_apoc:
        # Use APOC path expander with expandConfig to enforce linear execution
        single_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        CALL apoc.path.expandConfig(n, {
            relationshipFilter: 'CO_PURCHASED>',
            minLevel: 1,
            maxLevel: 1,
            uniqueness: 'NODE_GLOBAL',
            bfs: false
        }) YIELD path
        WITH DISTINCT last(nodes(path)) AS neighbor
        RETURN collect(neighbor.id) AS nodes
        """
    else:
        # Regular query with UNWIND to enforce linear execution
        single_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        WITH collect(n) AS nodes
        UNWIND nodes AS n
        MATCH (n)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """

    single_hop_result = session.run(single_hop_query, input_values=input_values)
    single_hop_nodes = single_hop_result.single()["nodes"]
    single_hop_nodes.sort()  # Sort the nodes

    end_time = time.time() * 1000000  # Convert to microseconds
    query_time = end_time - start_time
    query_times.append(query_time)

    all_levels.append(single_hop_nodes)
    results_file.write(str(single_hop_nodes) + "\n")
    stats_file.write(f"Single hop query time: {int(query_time)} us\n")

    # Query 2: Two hop query
    start_time = time.time() * 1000000  # Convert to microseconds

    if has_apoc:
        # Use APOC path expander for linear execution
        two_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        CALL apoc.path.expandConfig(n, {
            relationshipFilter: 'CO_PURCHASED>',
            minLevel: 2,
            maxLevel: 2,
            uniqueness: 'NODE_GLOBAL',
            bfs: false
        }) YIELD path
        WITH DISTINCT last(nodes(path)) AS neighbor
        RETURN collect(neighbor.id) AS nodes
        """
    else:
        # Regular query with UNWIND to force linear execution
        two_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        WITH collect(n) AS start_nodes
        UNWIND start_nodes AS n
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        WITH collect(level1) AS level1_nodes
        UNWIND level1_nodes AS level1
        MATCH (level1)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """

    two_hop_result = session.run(two_hop_query, input_values=input_values)
    two_hop_nodes = two_hop_result.single()["nodes"]
    two_hop_nodes.sort()  # Sort the nodes

    end_time = time.time() * 1000000  # Convert to microseconds
    query_time = end_time - start_time
    query_times.append(query_time)

    all_levels.append(two_hop_nodes)
    results_file.write(str(two_hop_nodes) + "\n")
    stats_file.write(f"Two hop query time: {int(query_time)} us\n")

    # Query 3: Three hop query
    start_time = time.time() * 1000000  # Convert to microseconds

    if has_apoc:
        # Use APOC path expander for linear execution
        three_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        CALL apoc.path.expandConfig(n, {
            relationshipFilter: 'CO_PURCHASED>',
            minLevel: 3,
            maxLevel: 3,
            uniqueness: 'NODE_GLOBAL',
            bfs: false
        }) YIELD path
        WITH DISTINCT last(nodes(path)) AS neighbor
        RETURN collect(neighbor.id) AS nodes
        """
    else:
        # Regular query with UNWIND pattern to force linear execution
        three_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        WITH collect(n) AS start_nodes
        UNWIND start_nodes AS n
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        WITH collect(level1) AS level1_nodes
        UNWIND level1_nodes AS level1
        MATCH (level1)-[:CO_PURCHASED]->(level2:Product)
        WITH collect(level2) AS level2_nodes
        UNWIND level2_nodes AS level2
        MATCH (level2)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """

    three_hop_result = session.run(three_hop_query, input_values=input_values)
    three_hop_nodes = three_hop_result.single()["nodes"]
    three_hop_nodes.sort()  # Sort the nodes

    end_time = time.time() * 1000000  # Convert to microseconds
    query_time = end_time - start_time
    query_times.append(query_time)

    all_levels.append(three_hop_nodes)
    results_file.write(str(three_hop_nodes) + "\n")
    stats_file.write(f"Three hop query time: {int(query_time)} us\n")

    # Query 4: Four hop query
    start_time = time.time() * 1000000  # Convert to microseconds

    if has_apoc:
        # Use APOC path expander for linear execution
        four_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        CALL apoc.path.expandConfig(n, {
            relationshipFilter: 'CO_PURCHASED>',
            minLevel: 4,
            maxLevel: 4,
            uniqueness: 'NODE_GLOBAL',
            bfs: false
        }) YIELD path
        WITH DISTINCT last(nodes(path)) AS neighbor
        RETURN collect(neighbor.id) AS nodes
        """
    else:
        # Regular query with UNWIND pattern to force linear execution
        four_hop_query = """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        WITH collect(n) AS start_nodes
        UNWIND start_nodes AS n
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        WITH collect(level1) AS level1_nodes
        UNWIND level1_nodes AS level1
        MATCH (level1)-[:CO_PURCHASED]->(level2:Product)
        WITH collect(level2) AS level2_nodes
        UNWIND level2_nodes AS level2
        MATCH (level2)-[:CO_PURCHASED]->(level3:Product)
        WITH collect(level3) AS level3_nodes
        UNWIND level3_nodes AS level3
        MATCH (level3)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """

    four_hop_result = session.run(four_hop_query, input_values=input_values)
    four_hop_nodes = four_hop_result.single()["nodes"]
    four_hop_nodes.sort()  # Sort the nodes

    end_time = time.time() * 1000000  # Convert to microseconds
    query_time = end_time - start_time
    query_times.append(query_time)

    all_levels.append(four_hop_nodes)
    results_file.write(str(four_hop_nodes) + "\n")
    stats_file.write(f"Four hop query time: {int(query_time)} us\n")

    # Calculate and write total time
    total_time = sum(query_times)
    stats_file.write(f"Total execution time: {int(total_time)} us\n")

    # Write stats about number of nodes per level
    stats_file.write(f"Number of levels: {len(all_levels)}\n")
    for i, level in enumerate(all_levels):
        stats_file.write(f"Level {i} nodes: {len(level)}\n")

    # Close files
    results_file.close()
    stats_file.close()

    # Close the session
    session.close()
    driver.close()

    print(f"Queries executed successfully")
    print(f"Results saved to Neo4J_results.txt")
    print(f"Stats saved to Neo4J_stats.txt")

if __name__ == "__main__":
    run_query_and_save_results()