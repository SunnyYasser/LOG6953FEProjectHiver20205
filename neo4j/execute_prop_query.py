from neo4j import GraphDatabase
from neo4j_auth import URI, USERNAME, PASSWORD


def run_query_and_save_results():
    # Open files for writing results and stats
    results_file = open("Neo4J_results.txt", "w")
    stats_file = open("Neo4J_stats.txt", "w")

    # Read affected buildings from input file
    with open("../input_query.txt", "r") as f:
        input_text = f.read().strip()

    # Process input - remove brackets and split by commas
    input_text = input_text.strip('[]')
    input_values = [int(x.strip()) for x in input_text.split(',') if x.strip()]

    all_levels = []
    query_times = []

    # Level 0 - starting buildings (directly affected)
    level0_nodes = input_values
    all_levels.append(sorted(level0_nodes))
    results_file.write(str(sorted(level0_nodes)) + "\n")

    # Inefficient Queries for different propagation levels
    queries = [
        """
        // Level 1 - One hop neighbors (Inefficient)
        CYPHER runtime = slotted
        PROFILE
        MATCH (n:Building)
        WHERE n.id IN $input_values
        WITH n
        MATCH (n)-[:CONNECTED_TO]->(neighbor:Building)
        WITH collect(neighbor.id) as neighbors
        UNWIND neighbors AS node
        WITH DISTINCT node as distinctNode
        RETURN collect(distinctNode) as nodes
        """,

        """
        // Level 2 - Two hop neighbors (Inefficient)
        CYPHER runtime = slotted
        PROFILE
        MATCH (n:Building)
        WHERE n.id IN $input_values
        WITH n
        MATCH (n)-[:CONNECTED_TO]->(m:Building)-[:CONNECTED_TO]->(neighbor:Building)
        WITH collect(neighbor.id) as neighbors
        UNWIND neighbors AS node
        WITH DISTINCT node as distinctNode
        RETURN collect(distinctNode) as nodes
        """,

        """
        // Level 3 - Three hop neighbors (Inefficient)
        CYPHER runtime = slotted
        PROFILE
        MATCH (n:Building)
        WHERE n.id IN $input_values
        WITH n
        MATCH (n)-[:CONNECTED_TO]->(m1:Building)-[:CONNECTED_TO]->(m2:Building)-[:CONNECTED_TO]->(neighbor:Building)
        WITH collect(neighbor.id) as neighbors
        UNWIND neighbors AS node
        WITH DISTINCT node as distinctNode
        RETURN collect(distinctNode) as nodes
        """,

        """
        // Level 4 - Four hop neighbors (Inefficient)
        CYPHER runtime = slotted
        PROFILE
        MATCH (n:Building)
        WHERE n.id IN $input_values
        WITH n
        MATCH (n)-[:CONNECTED_TO]->(m1:Building)-[:CONNECTED_TO]->(m2:Building)-[:CONNECTED_TO]->(m3:Building)-[:CONNECTED_TO]->(neighbor:Building)
        WITH collect(neighbor.id) as neighbors
        UNWIND neighbors AS node
        WITH DISTINCT node as distinctNode
        RETURN collect(distinctNode) as nodes
        """
    ]

    # Execute each query and measure time using Neo4j's PROFILE feature
    for i, query in enumerate(queries):
        # Create a new driver and session for each query - this ensures a fresh connection
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        session = driver.session(database="neo4j")

        try:
            # Try to clear query cache if the procedure exists
            try:
                session.run("CALL db.clearQueryCaches()")
            except Exception:
                # If procedure doesn't exist, continue without clearing
                pass

            # Execute the query with PROFILE to get execution timing
            result = session.run(query, input_values=input_values)

            # Get the result data
            record = result.single()
            nodes = record["nodes"]
            nodes.sort()  # Sort for consistent output

            # Get the query execution statistics
            summary = result.consume()

            # Get the execution time in microseconds (Neo4j reports in ms)
            query_time_ms = summary.result_available_after
            query_time_us = query_time_ms * 1000  # Convert ms to microseconds
            query_times.append(query_time_us)

            # Save results
            all_levels.append(nodes)
            results_file.write(str(nodes) + "\n")
            stats_file.write(f"Query {i + 1} time: {int(query_time_us)} us\n")
            print(f"Query {i + 1} execution time: {int(query_time_us)} us")

            # Try to clear stats if the procedure exists
            try:
                session.run("CALL db.stats.clear()")
            except Exception:
                # If procedure doesn't exist, continue without clearing
                pass

        except Exception as e:
            print(f"Error executing query {i + 1}: {str(e)}")
            stats_file.write(f"Query {i + 1} error: {str(e)}\n")
            # Add empty result for this level to maintain consistency
            all_levels.append([])
            results_file.write("[]\n")
        finally:
            # Always close the session and driver after each query
            session.close()
            driver.close()

    # Write summary statistics
    total_time = sum(query_times)
    stats_file.write(f"Total execution time: {int(total_time)} us\n")

    stats_file.write(f"Number of levels: {len(all_levels)}\n")
    for i, level in enumerate(all_levels):
        stats_file.write(f"Level {i} nodes: {len(level)}\n")

    # Close files
    results_file.close()
    stats_file.close()

    print("Queries executed successfully")
    print("Results saved to Neo4J_results.txt")
    print("Stats saved to Neo4J_stats.txt")


if __name__ == "__main__":
    run_query_and_save_results()