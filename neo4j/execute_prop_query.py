from neo4j import GraphDatabase
import time
from neo4j_auth import URI, USERNAME, PASSWORD


def run_query_and_save_results():
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
    session_config = {"database": "neo4j"}

    with open("../input_query.txt", "r") as f:
        input_text = f.read().strip()

    input_text = input_text.strip('[]')
    input_values = [int(x.strip()) for x in input_text.split(',') if x.strip()]

    session = driver.session(**session_config)

    all_levels = []
    query_times = []

    results_file = open("Neo4J_results.txt", "w")
    stats_file = open("Neo4J_stats.txt", "w")

    level0_nodes = input_values
    all_levels.append(sorted(level0_nodes))
    results_file.write(str(sorted(level0_nodes)) + "\n")

    queries = [
        """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        MATCH (n)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,
        """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        MATCH (level1)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,
        """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        MATCH (level1)-[:CO_PURCHASED]->(level2:Product)
        MATCH (level2)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,
        """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        MATCH (level1)-[:CO_PURCHASED]->(level2:Product)
        MATCH (level2)-[:CO_PURCHASED]->(level3:Product)
        MATCH (level3)-[:CO_PURCHASED]->(neighbor:Product)
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """
    ]

    for i, query in enumerate(queries):
        start_time = time.time() * 1_000_000
        result = session.run(query, input_values=input_values)
        nodes = result.single()["nodes"]
        nodes.sort()
        end_time = time.time() * 1_000_000
        query_time = end_time - start_time
        query_times.append(query_time)

        all_levels.append(nodes)
        results_file.write(str(nodes) + "\n")
        stats_file.write(f"Query {i + 1} time: {int(query_time)} us\n")
        print(f"Query {i + 1} execution time: {int(query_time)} us")

    total_time = sum(query_times)
    stats_file.write(f"Total execution time: {int(total_time)} us\n")

    stats_file.write(f"Number of levels: {len(all_levels)}\n")
    for i, level in enumerate(all_levels):
        stats_file.write(f"Level {i} nodes: {len(level)}\n")

    results_file.close()
    stats_file.close()
    session.close()
    driver.close()

    print("Queries executed successfully")
    print("Results saved to Neo4J_results.txt")
    print("Stats saved to Neo4J_stats.txt")


if __name__ == "__main__":
    run_query_and_save_results()
