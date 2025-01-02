# Databricks notebook source
# Define DBFS paths for bronze, silver, and gold
bronze_path = "/FileStore/sukesh_files/Energy_sample_data/Project/medalion/bronze"
silver_path = "/FileStore/sukesh_files/Energy_sample_data/Project/medalion/silverr"
gold_path = "/FileStore/sukesh_files/Energy_sample_data/Project/medalion/gold"


# COMMAND ----------

# Retrieve the environment from user input
dbutils.widgets.text(name="env", defaultValue="", label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# Function to create Bronze schema
def create_Bronze_Schema(environment, path):
    print("Using hive_metastore")
    #spark.sql("USE hive_metastore")
    print(f"Creating Bronze Schema in hive_metastore")
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `bronze` MANAGED LOCATION '{path}'""")

# COMMAND ----------

# Function to create Silver schema
def create_Silver_Schema(environment, path):
    print("Using hive_metastore")
    #spark.sql("USE hive_metastore")
    print(f"Creating Silver Schema in hive_metastore")
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{path}'""")

# COMMAND ----------

# Function to create Gold schema
def create_Gold_Schema(environment, path):
    print("Using hive_metastore")
    #spark.sql("USE hive_metastore")
    print(f"Creating Gold Schema in hive_metastore")
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{path}'""")

# COMMAND ----------

# Create the schemas
create_Bronze_Schema(env, bronze_path)
create_Silver_Schema(env, silver_path)
create_Gold_Schema(env, gold_path)