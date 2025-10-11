# This code assumes the connection variables (sql_jdbc_url, clickhouse_jdbc_url, etc.)
# have already been defined and are available in your Spark session.

# 1. Define the list of tables to transfer
source_tables = [
    "Tbale 1",
    "Table 2  ",
    "Table 3",
    "Table 4"
]

# 2. Define the target database in ClickHouse
clickhouse_database = " Db_name "

print(" Starting data transfer process...\n")

# 3. Loop through each table to read, count, and write
for table_name in source_tables:
    sql_full_table_name = f"dbo.{table_name}"
    clickhouse_full_table_name = f"{clickhouse_database}.{table_name}"
    
    print(f"--- Processing table: {sql_full_table_name} ---")
    
    df_sql = None  # Reset DataFrame for each loop

    # STEP 1: Read data from SQL Server
    print("  Step 1/3: Reading from SQL Server...")
    try:
        df_sql = spark.read.jdbc(
            url=sql_jdbc_url,
            table=sql_full_table_name,
            properties=sql_connection_properties
        )
        print(f"    -> Read successful.")
    except Exception as e:
        print(f"    -> ERROR reading table '{sql_full_table_name}': {e}")
        print(f"--- Skipping this table due to read error. ---\n")
        continue  # Skip to the next table

    # STEP 2: Count the records
    print("  Step 2/3: Counting records...")
    try:
        record_count = df_sql.count()
        # The {:,} format adds a comma separator for large numbers (e.g., 1,234,567)
        print(f"    -> Found {record_count:,} records to transfer.")
    except Exception as e:
        print(f"    -> ERROR counting records: {e}")

    # STEP 3: Write data to ClickHouse
    print(f"  Step 3/3: Writing to ClickHouse table '{clickhouse_full_table_name}'...")
    try:
        df_sql.write.jdbc(
            url=clickhouse_jdbc_url,
            table=clickhouse_full_table_name,
            mode="append",  # Use "append" to add data to the existing table
            properties=clickhouse_connection_properties
        )
        print("    -> Write successful.")
    except Exception as e:
        print(f"    -> ERROR writing to ClickHouse: {e}")
    
    print(f"--- Finished processing {sql_full_table_name}. ---\n")

print(" All tables have been processed. Transfer complete!")