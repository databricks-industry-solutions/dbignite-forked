# Databricks notebook source
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


def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def extract_entry_types(bundle):
    return [entry.get('resource', {}).get('resourceType') for entry in bundle.get('entry', [])]

# Map JSON data to entry types
bundles = map(read_json, sample_data_paths)
entry_types = sum(map(extract_entry_types, bundles), [])

# Count the occurrences of each entry type
entry_type_counts = Counter(entry_types)

# Print the entry type counts
print("Entry Type Distribution:")
for entry_type, count in entry_type_counts.items():
    print(f"{entry_type}: {count}")

# COMMAND ----------


import pandas as pd

def extract_care_plans(bundle):
    care_plans = []
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'CarePlan':
            care_plans.append(resource)
    return care_plans

# Map JSON data to care plans
bundles = map(read_json, sample_data_paths)
care_plans_by_patient = {}

for bundle in bundles:
    patient_id = None
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'Patient':
            patient_id = resource.get('id')
            break
    if patient_id:
        care_plans_by_patient[patient_id] = extract_care_plans(bundle)

# Create DataFrame from care plans
care_plan_data = []
for patient_id, care_plans in care_plans_by_patient.items():
    for care_plan in care_plans:
        care_plan_data.append({'PatientID': patient_id, 'CarePlan': care_plan})

care_plan_df = pd.DataFrame(care_plan_data)

# Print the DataFrame
print(care_plan_df)

# COMMAND ----------


def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def extract_care_plans(bundle):
    care_plans = []
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'CarePlan':
            care_plans.append(resource)
    return care_plans

# Map JSON data to care plans
bundles = map(read_json, sample_data_paths)
care_plans_by_patient = {}

for bundle in bundles:
    patient_id = None
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'Patient':
            patient_id = resource.get('id')
            break
    if patient_id:
        care_plans_by_patient[patient_id] = extract_care_plans(bundle)

# Create DataFrame from care plans
care_plan_data = []
for patient_id, care_plans in care_plans_by_patient.items():
    for care_plan in care_plans:
        care_plan_data.append({'PatientID': patient_id, 'CarePlan': care_plan})

care_plan_df = pd.DataFrame(care_plan_data)

# Print only the 'CarePlan' column
print(care_plan_df['CarePlan'])

# COMMAND ----------


