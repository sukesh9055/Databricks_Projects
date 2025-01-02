# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # To re-use common functions and variables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Re-Using functions
# MAGIC - We can re-use 2 functions here
# MAGIC   - Removing Duplicates
# MAGIC   - Removing NULLs

# COMMAND ----------

# MAGIC %md
# MAGIC # Defining all common variables
# MAGIC

# COMMAND ----------

checkpoint = '/FileStore/sukesh_files/Energy_sample_data/Project/checkpoints'
landing = '/FileStore/sukesh_files/Energy_sample_data/Project/landing'
bronze = '/FileStore/sukesh_files/Energy_sample_data/Project/medalion/bronze'
silver = '/FileStore/sukesh_files/Energy_sample_data/Project/medalion/silver'
gold = '/FileStore/sukesh_files/Energy_sample_data/Project/medalion/gold'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Defining common functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 01 -- Removing duplicates

# COMMAND ----------

def remove_Dups(df):
    print('Removing Duplicate values: ',end='')
    df_dup = df.dropDuplicates()
    print('Success!')
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 01 -- Handling NULLs

# COMMAND ----------

def handle_NULLs(df,Columns):
    print('Replacing NULLs of Strings DataType with "Unknown": ', end='')
    df_string = df.fillna('Unknown',subset=Columns)
    print('Success!')
    print('Replacing NULLs of Numeric DataType with "0":  ', end='')
    df_numeric = df_string.fillna(0,subset=Columns)
    print('Success!')
    return df_numeric

# COMMAND ----------

