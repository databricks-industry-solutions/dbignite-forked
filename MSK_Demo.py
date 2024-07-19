# Databricks notebook source
# MAGIC %pip install pyyaml
# MAGIC

# COMMAND ----------

import yaml
import json
import uuid
from datetime import datetime
from pyspark.sql import SparkSession

# Function to safely convert date
def safe_date_convert(date):
    try:
        return date.strftime("%Y-%m-%d") if date else None
    except AttributeError:
        return None

# Load the YAML configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Initialize a Spark session
spark = SparkSession.builder.appName("Fetch and Transform MSK Data").getOrCreate()

# Read a sample record from the Hive table
msk_record = spark.sql("SELECT * FROM hive_metastore.default.msk_adverse_events LIMIT 1").collect()[0].asDict()

# FHIR code mappings
SEVERITY_CODES = config['SEVERITY_CODES']
OUTCOME_CODES = config['OUTCOME_CODES']

# Function to transform an MSK record to a FHIR AdverseEvent JSON
def transform_to_fhir_adverse_event(msk_record):
    mappings = config['mappings']
    fhir_adverse_event = {
        "resourceType": config['fhir_adverse_event']['resourceType'],
        "id": str(uuid.uuid4()).lower(),
        "actuality": config['fhir_adverse_event']['actuality'],
        "text": config['fhir_adverse_event']['text']
    }

    # Applying mappings to create the FHIR JSON structure
    for key, (fhir_key, transform_expr) in mappings.items():
        try:
            if key in msk_record and msk_record[key] is not None:
                x = msk_record[key]
                transform = eval(transform_expr)
                if transform:  # Only add if transformed_value is not None
                    parts = fhir_key.split('.')
                    sub_dict = fhir_adverse_event
                    for part in parts[:-1]:
                        sub_dict = sub_dict.setdefault(part, {})
                    sub_dict[parts[-1]] = transform
        except Exception as e:
            print(f"Error processing {key}: {e}")

    return fhir_adverse_event

# Create the FHIR Bundle with the AdverseEvent entry
def create_fhir_bundle(adverse_event):
    bundle = {
        "resourceType": config['bundle']['resourceType'],
        "type": config['bundle']['type'],
        "entry": [
            {
                "fullUrl": config['bundle']['entry'][0]['fullUrl'].format(id=adverse_event['id']),
                "resource": adverse_event,
                "request": config['bundle']['entry'][0]['request']
            }
        ]
    }
    return bundle

# Transform MSK record and create FHIR Bundle
fhir_adverse_event = transform_to_fhir_adverse_event(msk_record)
fhir_bundle = create_fhir_bundle(fhir_adverse_event)

# Print the FHIR Bundle JSON for review
print(json.dumps(fhir_bundle, indent=4))


# COMMAND ----------


