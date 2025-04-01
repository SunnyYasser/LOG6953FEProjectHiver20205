from neo4j import GraphDatabase
from neo4j_auth import URI, USERNAME, PASSWORD

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def create_constraints():
    """Create uniqueness constraint for building nodes if it doesn't already exist."""
    with driver.session() as session:
        try:
            # Check if constraint already exists
            constraints = session.run("SHOW CONSTRAINTS").data()
            constraint_exists = any(
                constraint.get('name', '').startswith('constraint_') and 
                constraint.get('labelsOrTypes', []) == ['Building'] and
                constraint.get('properties', []) == ['id']
                for constraint in constraints
            )
            
            if not constraint_exists:
                # Create new constraint
                session.run("CREATE CONSTRAINT FOR (b:Building) REQUIRE b.id IS UNIQUE")
                print("✅ Building ID uniqueness constraint created")
            else:
                print("✅ Building ID uniqueness constraint already exists")
                
        except Exception as e:
            print(f"⚠️ Warning with constraint: {str(e)}")
            print("⚠️ Proceeding with import anyway...")

def import_building_data():
    """Batch imports the building connectivity dataset into Neo4j."""
    with driver.session() as session:
        batch_size = 10000
        edges = []
        batch_num = 0
        
        print("🔄 Starting building data import...")
        
        with open('../data.txt', 'r') as f:
            for line in f:
                if line.startswith('#'):  # Skip comments
                    continue
                    
                src, dst = map(int, line.strip().split())
                edges.append((src, dst))
                
                if len(edges) >= batch_size:
                    print(f"📦 Importing batch {batch_num} ({len(edges)} connections)")
                    insert_batch(session, edges)
                    edges = []
                    batch_num += 1
                    
        # Import any remaining edges
        if edges:
            print(f"📦 Importing final batch ({len(edges)} connections)")
            insert_batch(session, edges)
            
        print("✅ Building data import complete")

def insert_batch(session, edges):
    """Helper function to insert a batch of building connections."""
    query = """
    UNWIND $edges AS edge
    MERGE (b1:Building {id: edge[0]})
    MERGE (b2:Building {id: edge[1]})
    MERGE (b1)-[:CONNECTED_TO]->(b2)
    """
    session.write_transaction(lambda tx: tx.run(query, edges=edges))

def clear_database():
    """Remove all nodes, relationships and constraints from the database."""
    with driver.session() as session:
        try:
            # Drop all constraints first
            print("🧹 Dropping all constraints...")
            constraints = session.run("SHOW CONSTRAINTS").data()
            for constraint in constraints:
                name = constraint.get('name')
                if name:
                    session.run(f"DROP CONSTRAINT {name}")
            
            # Delete all relationships and nodes
            print("🧹 Removing all relationships and nodes...")
            session.run("MATCH (n) DETACH DELETE n")
            
            print("✅ Database cleared successfully")
            return True
        except Exception as e:
            print(f"❌ Error clearing database: {str(e)}")
            return False

def count_data():
    """Count and display database statistics."""
    with driver.session() as session:
        # Count buildings
        result = session.run("MATCH (b:Building) RETURN COUNT(b) AS count")
        building_count = result.single()["count"]
        
        # Count connections
        result = session.run("MATCH ()-[r:CONNECTED_TO]->() RETURN COUNT(r) AS count")
        connection_count = result.single()["count"]
        
        print(f"📊 Database contains {building_count} buildings with {connection_count} connections")

if __name__ == "__main__":
    try:
        print("🚀 Starting building data import process...")
        # Clear database first
        clear_database()
        # Create constraints
        create_constraints()
        # Import data
        import_building_data()
        # Show statistics
        count_data()
        print("✅ Import process completed successfully")
    except Exception as e:
        print(f"❌ Error during import: {str(e)}")
    finally:
        driver.close()
