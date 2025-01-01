# Databricks notebook source
# MAGIC %sql
# MAGIC select * from samples.tpch.customer limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog IF NOT EXISTS attribute_based_data_security  MANAGED LOCATION 'abfss://dev-bronze@ucmlopsbaselocation.dfs.core.windows.net/attribute_based_data_security';
# MAGIC create database IF NOT EXISTS attribute_based_data_security.bronze_layer;
# MAGIC create schema IF NOT EXISTS attribute_based_data_security.tpch;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table IF NOT EXISTS attribute_based_data_security.tpch.customer
# MAGIC (c_custkey bigint, 
# MAGIC  c_name string,
# MAGIC  c_address string,
# MAGIC  c_nation bigint,
# MAGIC  c_phone string,
# MAGIC  c_acctbal decimal(18,2),
# MAGIC  c_mktsegment string
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table attribute_based_data_security.tpch.customer alter column  c_name 
# MAGIC set tags ('pii'='true', 
# MAGIC           'domain'='customer', 
# MAGIC           'treatment'='encrypt-aes-gcm-nopadding');
# MAGIC
# MAGIC alter table attribute_based_data_security.tpch.customer alter column  c_address 
# MAGIC set tags ('pii'='true', 
# MAGIC           'domain'='customer', 
# MAGIC           'treatment'='pass-through');
# MAGIC
# MAGIC alter table attribute_based_data_security.tpch.customer alter column  c_phone 
# MAGIC set tags ('pii'='true', 
# MAGIC           'domain'='customer', 
# MAGIC           'treatment'='hash-sha256');
# MAGIC
# MAGIC alter table attribute_based_data_security.tpch.customer alter column  c_acctbal 
# MAGIC set tags ('pii'='false', 
# MAGIC           'domain'='customer', 
# MAGIC           'treatment'='pass-through', 
# MAGIC           'metrictype'='monetary-additive');
# MAGIC
# MAGIC alter table attribute_based_data_security.tpch.customer alter column  c_mktsegment 
# MAGIC set tags ('pii'='false', 
# MAGIC           'domain'='customer', 
# MAGIC           'treatment'='pass-through', 
# MAGIC           'owner'='marketing');

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view attribute_based_data_security.tpch.all_column_tags as 
# MAGIC with column_tags as 
# MAGIC (
# MAGIC   select catalog_name, schema_name, table_name, column_name,
# MAGIC          -- collect_list(map(tag_name, tag_value)) as tags
# MAGIC          map_from_arrays(collect_list(tag_name), collect_list(tag_value)) tags
# MAGIC from attribute_based_data_security.information_schema.column_tags
# MAGIC group by catalog_name, schema_name, table_name, column_name 
# MAGIC ),
# MAGIC column_metadata as 
# MAGIC (select c.table_catalog, c.table_schema, c.table_name, c.column_name,
# MAGIC        ct.tags
# MAGIC from attribute_based_data_security.information_schema.columns c 
# MAGIC  left outer join column_tags  ct 
# MAGIC on (c.table_catalog = ct.catalog_name 
# MAGIC      and c.table_schema = ct.schema_name 
# MAGIC      and c.table_name = ct.table_name 
# MAGIC      and c.column_name = ct.column_name)
# MAGIC )
# MAGIC select table_catalog, table_schema, table_name, column_name,tags
# MAGIC from column_metadata c 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from attribute_based_data_security.tpch.all_column_tags 
# MAGIC where table_catalog = 'attribute_based_data_security' and table_schema = 'tpch' and table_name = 'customer'

# COMMAND ----------


target_table = 'attribute_based_data_security.tpch.customer'

def get_tags(table):
    df = spark.table("attribute_based_data_security.tpch.all_column_tags") 
    df = df.filter(df.table_catalog == table.split('.')[0])
    df = df.filter(df.table_schema == table.split('.')[1])
    df = df.filter(df.table_name == table.split('.')[2])
    return  [row.asDict() for row in df.collect()]

tag_metadata = get_tags(target_table)

tag_metadata

# COMMAND ----------

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

def pii_encryption(column, domain):
    return 'abc' 

pii_encryption_udf = udf(lambda x, y: pii_encryption(x, y), StringType())

# COMMAND ----------

src = spark.table("samples.tpch.customer")

def get_treatments(column_name, tag_metadata):
    for m in tag_metadata:
        if column_name == m['column_name']:
            if m['tags']:
                return m['tags']
            else:
                return None 

for column_name in src.columns:
    print(column_name)
    all_tags = get_treatments(column_name=column_name, tag_metadata=tag_metadata)
    if not all_tags or all_tags['treatment'] == 'pass-through':
        pass     
    elif all_tags['treatment'] == 'encrypt-aes-gcm-nopadding':
        src = src.withColumn(column_name, pii_encryption_udf(src[column_name], lit(all_tags['domain']))) 

display(src)

# COMMAND ----------


