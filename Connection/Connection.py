# sql server config
sql_server_host = "172.16.68.4"
sql_server_port = "1433"
sql_database = "ELCM_QADB" 
sql_user = "testadmin"
sql_password = "Cner@321" 

sql_jdbc_url = f"jdbc:sqlserver://{sql_server_host}:{sql_server_port};databaseName={sql_database};trustServerCertificate=true"

sql_connection_properties = {
  "user": sql_user,
  "password": sql_password,
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# clcikhouse config
clickhouse_host = "172.16.3.6"
clickhouse_port = "8123"
clickhouse_database = "default"
clickhouse_user = "devadmin"
clickhouse_password = "Dev12837sadqdk"

clickhouse_jdbc_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_database}"

clickhouse_connection_properties = {
  "user": clickhouse_user,
  "password": clickhouse_password,
  "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# connecting to SQL Server and ClickHouse

print("--- Testing Database Connections ---")

# --- Test SQL Server Connection ---
print("\nAttempting to connect to SQL Server...")
try:
    spark.read.jdbc(
        url=sql_jdbc_url,
        table="dbo.EmployeeAttributeDetails",
        properties=sql_connection_properties
    ).limit(1).count()
    print("✅ Success: Connection to SQL Server is established.")
except Exception as e:
    print("❌ Failure: Could not connect to SQL Server.")
    print(f"   Error: {str(e)[:500]}")

# --- Test ClickHouse Connection ---
print("\nAttempting to connect to ClickHouse...")
try:
    test_query = "(SELECT 1) AS connection_test"
    spark.read.jdbc(
        url=clickhouse_jdbc_url,
        table=test_query,
        properties=clickhouse_connection_properties
    ).count()
    print("✅ Success: Connection to ClickHouse is established.")
except Exception as e:
    print("❌ Failure: Could not connect to ClickHouse.")
    print(f"   Error: {str(e)[:500]}")

