from neo4j import GraphDatabase
from neo4j_auth import URI, USERNAME, PASSWORD

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def create_constraints():
    """Create uniqueness constraint for Product nodes."""
    with driver.session() as session:
        session.run("CREATE CONSTRAINT FOR (p:Product) REQUIRE p.id IS UNIQUE;")

def import_amazon_data():
    """Batch imports the Amazon0601 dataset into Neo4j."""
    with driver.session() as session:
        batch_size = 10000
        edges = []
        batch_num = 0

        with open('../data.txt', 'r') as f:
            for line in f:
                if line.startswith('#'):  # Skip comments
                    continue

                src, dst = map(int, line.strip().split())
                edges.append((src, dst))

                if len(edges) >= batch_size:
                    print(f"Importing batch {batch_num}")
                    insert_batch(session, edges)
                    edges = []
                    batch_num += 1

        # Import any remaining edges
        if edges:
            print("Importing final batch")
            insert_batch(session, edges)

def insert_batch(session, edges):
    """Helper function to insert a batch of edges."""
    query = """
    UNWIND $edges AS edge
    MERGE (p1:Product {id: edge[0]})
    MERGE (p2:Product {id: edge[1]})
    MERGE (p1)-[:CO_PURCHASED]->(p2)
    """
    session.write_transaction(lambda tx: tx.run(query, edges=edges))

if __name__ == "__main__":
    create_constraints()  # Ensure constraints exist before importing
    import_amazon_data()
    driver.close()
