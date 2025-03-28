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
        MATCH (all:Product)
        WITH n, collect(all.id) as all_products
        MATCH (n)-[:CO_PURCHASED]->(neighbor:Product)
        WITH neighbor, all_products
        WHERE size(all_products) > 0 
        UNWIND all_products as unused
        WITH DISTINCT neighbor
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,

        """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        MATCH (allProducts:Product)
        WITH n, collect(allProducts.id) as all_prods
        WITH n, [x IN all_prods WHERE x > 0] as filtered_prods
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        MATCH (n)-[:CO_PURCHASED]->(level1Again:Product)
        WHERE level1.id = level1Again.id
        MATCH (level1)-[:CO_PURCHASED]->(neighbor:Product)
        WITH neighbor, toString(neighbor.id) as string_id
        WHERE size(string_id) > 0
        RETURN collect(DISTINCT neighbor.id) AS nodes
        """,

        """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        WITH n, count(*) as cnt
        MATCH (n)-[:CO_PURCHASED]->(level1:Product)
        MATCH (level1)-[:CO_PURCHASED]->(level2:Product)
        WITH level2
        MATCH (level2)-[:CO_PURCHASED]->(temp)
        WITH level2, count(temp) as degree
        WHERE degree > 0
        MATCH (level2)-[:CO_PURCHASED]->(neighbor:Product)
        WITH neighbor
        WHERE neighbor.id IS NOT NULL
        WITH COLLECT(neighbor.id) as ids
        UNWIND ids as id
        WITH DISTINCT id as uniqueId
        RETURN collect(uniqueId) AS nodes
        """,

        """
        MATCH (n:Product)
        WHERE n.id IN $input_values
        MATCH (unused:Product)
        WITH n, count(unused) as cnt
        MATCH path1 = (n)-[:CO_PURCHASED]->(level1:Product)
        MATCH path2 = (level1)-[:CO_PURCHASED]->(level2:Product)
        MATCH path3 = (level2)-[:CO_PURCHASED]->(level3:Product)
        WITH level3, [p IN [path1, path2, path3] | length(p)] as pathLengths
        WHERE exists((level3)-[:CO_PURCHASED]->())
        MATCH (level3)-[:CO_PURCHASED]->(neighbor:Product)
        WITH neighbor, size(toString(neighbor.id)) as id_length
        WHERE id_length > 0
        WITH collect(neighbor.id) as neighbor_ids
        UNWIND neighbor_ids as neighbor_id
        WITH DISTINCT neighbor_id as unique_id
        ORDER BY unique_id
        RETURN collect(unique_id) AS nodes
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