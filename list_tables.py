from google.cloud import bigquery
import pandas as pd

def list_tables_with_size_and_last_modified(project_id):
    # Initialize a BigQuery client
    client = bigquery.Client(project=project_id)

    # Get a list of datasets in the project
    datasets = client.list_datasets()

    table_data = []

    for dataset in datasets:
        dataset_id = dataset.dataset_id
        dataset_ref = client.dataset(dataset_id)

        # Get a list of tables in each dataset
        tables = client.list_tables(dataset_ref)

        for table in tables:
            table_ref = dataset_ref.table(table.table_id)
            table_info = client.get_table(table_ref)

            table_name = table.table_id
            table_size_gb = table_info.num_bytes / (1024**3)  # Convert size from bytes to GB
            last_modified = table_info.modified  # Timestamp of last modification

            table_data.append({
                "dataset_name": dataset_id,
                "table_name": table_name,
                "table_size_gb": round(table_size_gb, 2),  # Round to 2 decimal places
                "last_modified": last_modified.replace(tzinfo=None)
            })

    return table_data

def save_to_excel(table_data, file_name):
    # Create a DataFrame from the table data
    df = pd.DataFrame(table_data)

    # Save the DataFrame to an Excel file
    df.to_excel(file_name, index=False)
    print(f"Data successfully saved to {file_name}")

# Example usage
if __name__ == "__main__":
    for project_id in ("gcp-wow-rwds-ai-msf-dev", "gcp-wow-rwds-ai-safari-prod", "gcp-wow-rwds-ai-safari-dev", "gcp-wow-rwds-ai-msf-prod"):
        tables = list_tables_with_size_and_last_modified(project_id)
        save_to_excel(tables, f"{project_id}.xlsx")
