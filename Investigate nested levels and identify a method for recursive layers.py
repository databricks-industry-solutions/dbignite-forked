# Databricks notebook source
# MAGIC %md
# MAGIC ###Version 1

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel
from dbignite.readers import read_from_directory
from dbignite.readers import FhirFormat

fhir_schema = FhirSchemaModel()
patient_schema = fhir_schema.schema("Patient")

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DataType, ArrayType, MapType
)
from typing import Any, List, Dict, get_type_hints

def get_python_type(data_type: DataType) -> Any:
    if isinstance(data_type, IntegerType):
        return int
    elif isinstance(data_type, StringType):
        return str
    elif isinstance(data_type, StructType):
        return dict
    elif isinstance(data_type, ArrayType):
        return List[get_python_type(data_type.elementType)]
    elif isinstance(data_type, MapType):
        return Dict[str, get_python_type(data_type.valueType)]
    else:
        return Any

def flatten_schema(schema: StructType, prefix: str = '') -> List[tuple]:
    fields = []
    for field in schema.fields:
        field_name = f"{prefix}{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            fields.extend(flatten_schema(field.dataType, f"{field_name}_"))
        else:
            fields.append((field_name, get_python_type(field.dataType)))
    return fields

def generate_class_from_schema(schema: StructType, class_name: str) -> type:
    # Flatten the schema
    flat_fields = flatten_schema(schema)

    # Define the __init__ method
    def __init__(self, **kwargs):
        for field_name, field_type in flat_fields:
            value = kwargs.get(field_name)
            if value is not None:
                if isinstance(field_type, list):
                    element_type = field_type[0]
                    value = [element_type(v) for v in value]
                elif isinstance(field_type, dict):
                    value = {k: str(v) for k, v in value.items()}  # Convert all values to strings
              
            setattr(self, field_name, value)

    # Define the __repr__ method
    def __repr__(self):
        field_values = ", ".join([f"{field_name}={{self.{field_name}}}" for field_name, _ in flat_fields])
        return f"{class_name}({field_values})".format(self=self)

    # Create the class dictionary
    class_dict = {
        '__init__': __init__,
        '__repr__': __repr__,
    }

    return type(class_name, (object,), class_dict)

# Example usage
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", IntegerType(), True)
    ]), True),
    StructField("emails", ArrayType(StringType()), True),
    StructField("phone_numbers", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("number", StringType(), True)
    ])), True),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

# Generate class from schema
Person = generate_class_from_schema(schema, "Person")

# COMMAND ----------

# Sample data
sample_data = {
    "id": 1,
    "name": "John",
    "address_city": "New York",
    "address_state": "NY",
    "address_zip_code": 10001,
    "emails": ["john@example.com", "john.doe@example.com"],
    "phone_numbers": [
        {"type": "home", "number": "123-456-7890"},
        {"type": "work", "number": "987-654-3210"}
    ]
}

# Create an instance of Person class
person = Person(**sample_data)

# Print the person object
print(person)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Version 2

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DataType, MapType
from typing import Any, List, Dict

def get_python_type(data_type: DataType) -> Any:
    if isinstance(data_type, IntegerType):
        return int
    elif isinstance(data_type, StringType):
        return str
    elif isinstance(data_type, StructType):
        return dict
    elif isinstance(data_type, ArrayType):
        return List[get_python_type(data_type.elementType)]
    elif isinstance(data_type, MapType):
        return Dict[str, get_python_type(data_type.valueType)]
    else:
        return Any

def flatten_data(data, parent_key='', sep='_'):
    items = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_data(v, new_key, sep))
            else:
                items[new_key] = v
    elif isinstance(data, list):
        for index, element in enumerate(data):
            new_key = f"{parent_key}{sep}{index}"
            if isinstance(element, (dict, list)):
                items.update(flatten_data(element, new_key, sep))
            else:
                items[new_key] = element
    return items

class PatientData:
    def __init__(self, data):
        for key, value in data.items():
            setattr(self, key, value)

    def __repr__(self):
        attrs = [f"{key}={getattr(self, key)!r}" for key in self.__dict__]
        return f"{self.__class__.__name__}({', '.join(attrs)})" 

class CondensedPatientDetails:
    def __init__(self, patient_data):
        self.patient_data = patient_data

    def __str__(self):
        details = (f"Condensed Patient Details:\n"
                   f"Patient: {getattr(self.patient_data, 'name_0_family', 'Unknown')} {getattr(self.patient_data, 'name_0_given_0', 'Unknown')}\n"
                   f"Gender: {getattr(self.patient_data, 'gender', 'Unknown')}\n"
                   f"Birth Date: {getattr(self.patient_data, 'birthDate', 'Unknown')}\n"
                   f"Address: {getattr(self.patient_data, 'address_0_line_0', 'Unknown')}, {getattr(self.patient_data, 'address_0_city', 'Unknown')}, "
                   f"{getattr(self.patient_data, 'address_0_state', 'Unknown')}, {getattr(self.patient_data, 'address_0_postalCode', 'Unknown')}, {getattr(self.patient_data, 'address_0_country', 'Unknown')}\n"
                   f"Relationship Status: {getattr(self.patient_data, 'maritalStatus_coding_0_display', 'Unknown')}\n"
                   f"Primary Language: {getattr(self.patient_data, 'communication_0_language_text', 'Unknown')}\n"
                   f"Contact Phone: {getattr(self.patient_data, 'telecom_0_value', 'Unknown')}\n"
                   f"Driver's License: {getattr(self.patient_data, 'identifier_3_value', 'N/A')}\n"
                   f"Passport Number: {getattr(self.patient_data, 'identifier_4_value', 'N/A')}\n"
                   f"Medical Record Number: {getattr(self.patient_data, 'identifier_1_value', 'N/A')}\n"
                   f"Social Security Number: {getattr(self.patient_data, 'identifier_2_value', 'N/A')}\n"
                   f"Mother's Maiden Name: {getattr(self.patient_data, 'extension_2_valueString', 'N/A')}\n"
                   f"Race: {getattr(self.patient_data, 'extension_0_extension_0_valueCoding_display', 'N/A')}\n"
                   f"Ethnicity: {getattr(self.patient_data, 'extension_1_extension_0_valueCoding_display', 'N/A')}\n")
        return details

# Load and process the JSON data from a file
file_path = 'sampledata/Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json'

try:
    with open(file_path, 'r') as file:
        data = json.load(file)
except FileNotFoundError:
    print(f"File not found: {file_path}")
    data = None

if data:
    # Extract patient resource from the bundle
    patient_data = None
    for entry in data.get('entry', []):
        if entry.get('resource', {}).get('resourceType') == 'Patient':
            patient_data = entry['resource']
            break

    if patient_data:
        # Clean and flatten the patient data
        cleaned_data = flatten_data(patient_data)

        # Print to debug
        print("Patient:", cleaned_data)

        # Create an instance of PatientData with the flattened patient data
        patient_data_instance = PatientData(cleaned_data)

        # Create an instance of CondensedPatientDetails
        condensed_details = CondensedPatientDetails(patient_data_instance)
        print(condensed_details)
    else:
        print("No patient resource found in the bundle.")
else:
    print("No data found.")
