# Databricks notebook source
# MAGIC %md
# MAGIC Separate Output of JSON Entries from Multiple Bundles

# COMMAND ----------

import json
import os
from collections import Counter
import pandas as pd


sample_data_paths = [
    "sampledata/Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json",
    "sampledata/Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json",
    "sampledata/Abraham_Wiza_1f9211d6-4232-4e9e-0e9b-37b18575e22f.json"
]
def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def extract_entries(bundle):
    return bundle.get('entry', [])

bundles = map(read_json, sample_data_paths)
entries = map(extract_entries, bundles)

def print_entries(entries):
    for entry in entries:
        for e in entry:
            print(e)

entries_list = list(entries)
print_entries(entries_list)


# COMMAND ----------

# MAGIC %md
# MAGIC Splitting FHIR Bundles into Separate NDJSON Files

# COMMAND ----------

import json
import os

sample_data_paths = [
    "sampledata/Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json",
    "sampledata/Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json",
    "sampledata/Abraham_Wiza_1f9211d6-4232-4e9e-0e9b-37b18575e22f.json"
]

output_directory = "Patient File Test/Finaltest"

os.makedirs(output_directory, exist_ok=True)

def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def write_bundle_to_ndjson(args):
    bundle, output_file_path = args
    with open(output_file_path, 'w') as f:
        for entry in bundle.get('entry', []):
            if entry:
                entry_json = json.dumps(entry)
                f.write(entry_json + '\n')

bundles = map(read_json, sample_data_paths)
output_file_paths = [os.path.join(output_directory, f"bundle_{i}final.ndjson") for i in range(1, len(sample_data_paths) + 1)]

list(map(write_bundle_to_ndjson, zip(bundles, output_file_paths)))

for i, output_file_path in enumerate(output_file_paths, 1):
    print(f"Bundle {i} saved successfully to: {output_file_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC Merging FHIR Bundles into a Single NDJSON File

# COMMAND ----------

import json
import os

sample_data_paths = [
   "sampledata/Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json",
    "sampledata/Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json",
    "sampledata/Abraham_Wiza_1f9211d6-4232-4e9e-0e9b-37b18575e22f.json"
]

output_file_path = "Patient File Test/Finaltest.ndjson"

def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def write_bundle_to_ndjson(bundles, output_file_path):
    with open(output_file_path, 'a') as f:
        for bundle in bundles:
            for entry in bundle.get('entry', []):
                if entry: 
                    entry_json = json.dumps(entry)
                    f.write(entry_json + '\n')

bundles = list(map(read_json, sample_data_paths))

write_bundle_to_ndjson(bundles, output_file_path)

print(f"All patient data saved successfully to: {output_file_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC fhir to ndjson 

# COMMAND ----------

import os
# Define df
from dbignite.readers import read_from_directory

sample_data_1 = "../sampledata/Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json"
sample_data_2 = "../sampledata/Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json"
sample_data_3 = "../sampledata/Abraham_Wiza_1f9211d6-4232-4e9e-0e9b-37b18575e22f.json"
tmp_path = "/tmp/sampledata"
# dbutils.fs.cp("file:" + sample_data_1, tmp_path + "/temp1.json", True)
# dbutils.fs.cp("file:" + sample_data_2, tmp_path + "/temp2.json", True)
# dbutils.fs.cp("file:" + sample_data_3, tmp_path + "/temp3.json", True)
sample_data_1 = os.path.abspath(sample_data_1)
sample_data_2 = os.path.abspath(sample_data_2)
sample_data_3 = os.path.abspath(sample_data_3)

bundle = read_from_directory(tmp_path)
df = bundle.entry()

# Define df_to_ndjson function
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, lit, struct, to_json, regexp_replace

def df_to_ndjson(df: DataFrame, resource_type: str) -> DataFrame:
    return (df
            .withColumn(resource_type, explode(resource_type))
            .withColumn("resourceType", lit(resource_type))
            .select(
                to_json(
                    struct(
                        lit(resource_type).alias("resourceType"), 
                        f"{resource_type}.*"
                    )
                ).alias("ndjson")
            )
            .withColumn("ndjson", regexp_replace("ndjson", "\\\\(.?)", "$1"))
            # .selectExpr("concat(ndjson, '\\n') as ndjson")  # Add newline delimiter
            )

# Call df_to_ndjson function 
resource_types = ["Encounter", "Claim"]

ndjson_objects = {}
for rtype in resource_types:
    transformed_df = df_to_ndjson(df, rtype)
    ndjson_objects[rtype] = transformed_df.collect()

# Print or use ndjson_objects as needed
ndjson_objects


# COMMAND ----------

