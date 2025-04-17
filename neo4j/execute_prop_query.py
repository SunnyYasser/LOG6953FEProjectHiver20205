from neo4j import GraphDatabase
from neo4j_auth import URI, USERNAME, PASSWORD


def run_query_and_save_results():
    results_file = open("Neo4J_results.txt", "w")
    stats_file = open("Neo4J_stats.txt", "w")

    with open("../input_query.txt", "r") as f:
        input_text = f.read().strip()

    input_text = input_text.strip('[]')
    input_values = [int(x.strip()) for x in input_text.split(',') if x.strip()]

    all_levels = []
    first_result_times = []
    last_result_times = []
    total_times = []

    level0_nodes = input_values
    all_levels.append(sorted(level0_nodes))
    results_file.write(str(sorted(level0_nodes)) + "\n")

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

    for i, query in enumerate(queries):
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        session = driver.session(database="neo4j")

        try:
            try:
                session.run("CALL db.clearQueryCaches()")
            except Exception:
                pass

            result = session.run(query, input_values=input_values)

            nodes = []
            for record in result:
                nodes.extend(record["nodes"])

            summary = result.consume()

            total_exec_time_ms = summary.result_consumed_after
            total_exec_time_us = total_exec_time_ms * 1000
            total_times.append(total_exec_time_us)

            nodes = sorted(list(set(nodes)))
            all_levels.append(nodes)
            results_file.write(str(nodes) + "\n")

            stats_file.write(f"Query {i + 1} time (total execution time): {int(total_exec_time_us)} us\n")

            print(f"Query {i + 1} execution time:")
            print(f"{int(total_exec_time_us)} us")

            try:
                session.run("CALL db.stats.clear()")
            except Exception:
                pass

        except Exception as e:
            print(f"Error executing query {i + 1}: {str(e)}")
            stats_file.write(f"Query {i + 1} error: {str(e)}\n")
            all_levels.append([])
            results_file.write("[]\n")
            first_result_times.append(0)
            last_result_times.append(0)
            total_times.append(0)
        finally:
            session.close()
            driver.close()

    stats_file.write("\nSUMMARY STATISTICS\n")
    stats_file.write("=================\n")

    total_execution_time = sum(total_times)

    stats_file.write(f"Total Execution Time (all queries): {int(total_execution_time)} us\n")

    stats_file.write(f"\nNumber of levels: {len(all_levels)}\n")
    for i, level in enumerate(all_levels):
        stats_file.write(f"Level {i} nodes: {len(level)}\n")

    results_file.close()
    stats_file.close()

    print("\nQueries executed successfully")
    print("Results saved to Neo4J_results.txt")
    print("Stats saved to Neo4J_stats.txt")


if __name__ == "__main__":
    run_query_and_save_results()
