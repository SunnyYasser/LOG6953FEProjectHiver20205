from neo4j import GraphDatabase
import time
from neo4j_auth import URI, USERNAME, PASSWORD


def run_query_and_save_results():
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
    session_config = {"database": "neo4j"}

    # Read affected buildings from input file
    with open("../input_query.txt", "r") as f:
        input_text = f.read().strip()

    # Process input - remove brackets and split by commas
    input_text = input_text.strip('[]')
    input_values = [int(x.strip()) for x in input_text.split(',') if x.strip()]

    all_levels = []
    query_times = []

    # Open files for writing results and stats
    results_file = open("Neo4J_results.txt", "w")
    stats_file = open("Neo4J_stats.txt", "w")

    # Level 0 - starting buildings (directly affected)
    level0_nodes = input_values
    all_levels.append(sorted(level0_nodes))
    results_file.write(str(sorted(level0_nodes)) + "\n")

    # Queries for different propagation levels
    queries = [
        """
        // Level 1 - One hop neighbors
        MATCH (n:Building)
        WHERE n.id IN $input_values
        MATCH (n)-[:CONNECTED_TO]->(neighbor:Building)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,

        """
        // Level 2 - Two hop neighbors
        MATCH (n:Building)
        WHERE n.id IN $input_values
        MATCH (n)-[:CONNECTED_TO]->()-[:CONNECTED_TO]->(neighbor:Building)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,

        """
        // Level 3 - Three hop neighbors
        MATCH (n:Building)
        WHERE n.id IN $input_values
        MATCH (n)-[:CONNECTED_TO]->()-[:CONNECTED_TO]->()-[:CONNECTED_TO]->(neighbor:Building)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,

        """
        // Level 4 - Four hop neighbors
        MATCH (n:Building)
        WHERE n.id IN $input_values
        MATCH (n)-[:CONNECTED_TO]->()-[:CONNECTED_TO]->()-[:CONNECTED_TO]->()-[:CONNECTED_TO]->(neighbor:Building)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """
    ]

    # Execute each query and measure time
    for i, query in enumerate(queries):
        session = driver.session(**session_config)
        
        try:
            start_time = time.time() * 1_000_000  # Convert to microseconds
            result = session.run(query, input_values=input_values)
            nodes = result.single()["nodes"]
            nodes.sort()  # Sort for consistent output
            end_time = time.time() * 1_000_000
            query_time = end_time - start_time
            query_times.append(query_time)

            all_levels.append(nodes)
            results_file.write(str(nodes) + "\n")
            stats_file.write(f"Query {i + 1} time: {int(query_time)} us\n")
            print(f"Query {i + 1} execution time: {int(query_time)} us")
        except Exception as e:
            print(f"Error executing query {i + 1}: {str(e)}")
            stats_file.write(f"Query {i + 1} error: {str(e)}\n")
            # Add empty result for this level to maintain consistency
            all_levels.append([])
            results_file.write("[]\n")
        finally:
            # Always close the session
            session.close()

    # Write summary statistics
    total_time = sum(query_times)
    stats_file.write(f"Total execution time: {int(total_time)} us\n")

    stats_file.write(f"Number of levels: {len(all_levels)}\n")
    for i, level in enumerate(all_levels):
        stats_file.write(f"Level {i} nodes: {len(level)}\n")

    # Close files and database connection
    results_file.close()
    stats_file.close()
    driver.close()

    print("Queries executed successfully")
    print("Results saved to Neo4J_results.txt")
    print("Stats saved to Neo4J_stats.txt")


if __name__ == "__main__":
    run_query_and_save_results()
