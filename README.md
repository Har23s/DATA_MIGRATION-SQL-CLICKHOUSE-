On prem SQL Server to ClickHouse Data Migration Guide
This document provides a complete, generalized guide for migrating tables from a SQL Server database to a ClickHouse database using a PySpark notebook in Azure Databricks.

Project Overview
The goal of this project is to perform a bulk data transfer of specified tables from a source SQL Server database to a destination ClickHouse database. The process leverages the distributed computing power of Spark to read from the source and write to the destination efficiently.

Source: SQL Server

Destination: ClickHouse

Orchestration Tool: Azure Databricks (Pyspark)

1. Databricks Cluster Configuration
Before you begin, ensure your Databricks cluster is set up correctly.

Databricks Runtime Version: 13.3 LTS or higher is recommended.

Cluster Type: Use a Standard or Memory Optimized cluster, depending on the volume of data you are migrating.

Required Libraries 
You must install the necessary JDBC drivers on your cluster to enable the connection to both databases.
Navigate to your cluster's settings and select the Libraries tab
Click Install new and install the following two libraries from Maven.

A. SQL Server JDBC Driver
This allows Spark to connect to your SQL Server instance.

Source: Maven

Coordinates: com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre8

B. ClickHouse JDBC Driver
This allows Spark to connect to your ClickHouse instance.

Source: Maven

Coordinates: com.clickhouse:clickhouse-jdbc:0.5.0

Important: After installing the libraries, restart your cluster to ensure they are loaded and active.

 2. Setup and Configuration
To manage credentials securely, we'll use Databricks Secrets.

Create a Secret Scope
First, create a secret scope to hold your database credentials.

Navigate to the URL: https://<your-databricks-instance>#secrets/createScope

Enter a Scope Name (e.g., db-migration-scope) and click Create.

Store Credentials as Secrets
Add the following secrets to your newly created scope using the Databricks CLI or UI:

sql-user: Your SQL Server username.

sql-password: Your SQL Server password.

clickhouse-user: Your ClickHouse username.

clickhouse-password: Your ClickHouse password.

 3.  Execution Flow
Follow these steps in order to perform the migration.

Step 1: Create Destination Tables in ClickHouse (DDL)
Before running the migration script, you must create the table structures in your ClickHouse database. You'll need to translate your SQL Server CREATE TABLE statements to the ClickHouse syntax.

Key Translation Rules:

Engine: Every table in ClickHouse needs an engine. Use ENGINE = MergeTree() for most use cases.

Primary Key: The SQL Server PRIMARY KEY CLUSTERED becomes the ORDER BY clause in ClickHouse. This is crucial for performance.

No IDENTITY: ClickHouse does not support auto-incrementing columns. Define these columns as standard integer types (e.g., Int32, Int64) and handle ID generation in your application.

No Foreign Keys: ClickHouse does not enforce foreign key constraints. Simply omit them from your CREATE TABLE statement.

Data Types: Map SQL Server types to ClickHouse types (e.g., varchar -> String, datetime -> DateTime, bit -> UInt8).

Nullability: Columns that allow NULL values must be explicitly wrapped in the Nullable() function (e.g., Nullable(String)).

Example Translation:

<details>
Click to view an example SQL Server to ClickHouse DDL conversion

SQL Server Source:

SQL

CREATE TABLE dbo.Products (
    ProductID INT IDENTITY(1,1) NOT NULL,
    ProductName VARCHAR(100) NOT NULL,
    CategoryID INT NULL,
    Price FLOAT NULL,
    Discontinued BIT NOT NULL,
    CONSTRAINT PK_Products PRIMARY KEY CLUSTERED (ProductID)
);
ClickHouse Destination:

SQL

CREATE TABLE your_database.Products
(
    `ProductID` Int32,
    `ProductName` String,
    `CategoryID` Nullable(Int32),
    `Price` Nullable(Float64),
    `Discontinued` UInt8
)
ENGINE = MergeTree()
ORDER BY (ProductID);
</details>

Step 2: Run the PySpark Migration Script
Create a new notebook in Databricks and paste the following code. This script is designed to be flexible and handles tables from different source schemas (dbo, ED, Common, etc.).


Step 3: Verify the Data
After the script finishes, run COUNT(*) queries on a few tables in both SQL Server and ClickHouse to ensure the number of records matches and the migration was successful.

Example Verification in ClickHouse:
SQL
SELECT count(*) FROM your_database.YourTable1;









