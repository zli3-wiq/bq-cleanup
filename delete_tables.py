import csv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def delete_table_if_exists(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    # Construct the fully qualified table ID (project.dataset.table)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        # Check if the table exists by attempting to fetch the table's metadata
        client.get_table(table_ref)  # This will raise NotFound if the table does not exist
        print(f"Table {table_ref} exists, proceeding to delete it.")
        
        # Delete the table
        client.delete_table(table_ref)
        print(f"Deleted table: {table_ref}")
    
    except NotFound:
        print(f"Table {table_ref} does not exist, skipping deletion.")
    except Exception as e:
        print(f"Error deleting table {table_ref}: {e}")

def delete_tables_from_csv(csv_file):
    with open(csv_file, mode='r') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            project_id = row['Project']
            dataset_id = row['Dataset']
            table_id = row['Table']

            delete_table_if_exists(project_id, dataset_id, table_id)

# Example usage
if __name__ == "__main__":
    # Path to your CSV file with columns: Project, Dataset, Table
    csv_file = 'to_remove.csv'
    
    delete_tables_from_csv(csv_file)
