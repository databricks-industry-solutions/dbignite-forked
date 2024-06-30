# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FHIR Patient Data") \
    .getOrCreate()

class PatientData:
    """A class that retrieves and processes patient data from FHIR Bundles (Class A)."""

    def __init__(self, patient_id, patient_name):
        self.patient_id = patient_id
        self.patient_name = patient_name
        self.patient_file_name = f"{patient_name.replace(' ', '_')}_{patient_id}.json"
        self.resource = self._load_resource()

    def _load_resource(self):
        """Load the patient resource from the JSON file, checking file existence."""
        path = os.path.join("/Workspace/Users/islam.hoti@xponentl.ai/dbignite/sampledata", self.patient_file_name)
        if not os.path.exists(path):
            print(f"File not found: {path}")
            return None
        try:
            with open(path, 'r') as jf:
                file = json.load(jf)
                return file.get('entry', [{}])[0].get('resource', None)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None
        except IOError as e:
            print(f"I/O error: {e}")
            return None

    def _get_identifier(self, code):
        """Retrieve identifier based on code."""
        return next((id['value'] for id in self.resource.get('identifier', []) if any(c['code'] == code for c in id.get('type', {}).get('coding', []))), None)

    def _get_extension(self, url):
        """Retrieve extension based on URL."""
        ext = next((ext for ext in self.resource.get('extension', []) if ext.get('url') == url), None)
        if ext and 'extension' in ext:
            return next((nested_ext.get('valueString') for nested_ext in ext['extension'] if 'valueString' in nested_ext), None)
        return ext.get('valueString') if ext else None

    def get_data(self):
        """Extract and return patient data transformed into the desired schema."""
        patient_data = {
            "id": self.resource.get('id', None),
            "name": " ".join([self.resource.get('name', [{}])[0].get('given', [''])[0], self.resource.get('name', [{}])[0].get('family', '')]).strip(),
            "birth_date": self.resource.get('birthDate', None),
            "gender": self.resource.get('gender', None),
            "location": ", ".join(filter(None, [self.resource.get('address', [{}])[0].get('line', [''])[0], self.resource.get('address', [{}])[0].get('city', ''), self.resource.get('address', [{}])[0].get('state', ''), self.resource.get('address', [{}])[0].get('postalCode', '')])).strip(),
            "relationship_status": self.resource.get('maritalStatus', {}).get('text', None),
            "primary_language": self.resource.get('communication', [{}])[0].get('language', {}).get('text', None),
            "contact_phone": next((contact['value'] for contact in self.resource.get('telecom', []) if contact.get('system') == 'phone'), None),
            "driver_license": self._get_identifier('DL'),
            "passport_number": self._get_identifier('PPN'),
            "medical_record_number": self._get_identifier('MR'),
            "social_security_number": self._get_identifier('SS'),
            "mothers_maiden_name": self._get_extension('http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName'),
            "race": self._get_extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'),
            "ethnicity": self._get_extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity')
        }
        return patient_data

    def to_row(self):
        """Convert the transformed patient data to a Spark Row."""
        patient_data = self.get_data()
        return Row(**patient_data)

    def to_class_b(self):
        """Transform the patient data into Class B."""
        if not self.resource:
            return None
        given = self.resource.get('name', [{}])[0].get('given', [''])[0]
        family = self.resource.get('name', [{}])[0].get('family', '')
        patient_data = self.get_data()
        return ClassB(
            self.patient_id,
            given,
            family,
            patient_data["birth_date"],
            patient_data["gender"],
            patient_data["location"],
            patient_data["relationship_status"],
            patient_data["primary_language"],
            patient_data["contact_phone"],
            patient_data["driver_license"],
            patient_data["passport_number"],
            patient_data["medical_record_number"],
            patient_data["social_security_number"],
            patient_data["mothers_maiden_name"],
            patient_data["race"],
            patient_data["ethnicity"]
        )

# Transformation function f(x)
def transform_patient_data_to_class_b(patient_data_instance):
    return patient_data_instance.to_class_b()

# Load your initial data into a Spark DataFrame
# Assuming df is your initial Spark DataFrame
df = spark.createDataFrame([
    Row(patient_id='4a0bf980-a2c9-36d6-da55-14d7aa5a85d9', patient_name='Abe Bernhard'),
    # Add more rows as needed
])

# Convert Spark DataFrame to RDD[PatientData]
rdd_class_a = df.rdd.map(lambda row: PatientData(row.patient_id, row.patient_name))

# Transformation f(x): RDD[PatientData] -> RDD[Class B]
rdd_class_b = rdd_class_a.map(transform_patient_data_to_class_b)

class ClassB:
    """A placeholder for Class B schema."""
    def __init__(self, patient_id, given, family, birth_date, gender, location, relationship_status, primary_language, contact_phone, driver_license, passport_number, medical_record_number, social_security_number, mothers_maiden_name, race, ethnicity):
        self.patient_id = patient_id
        self.given = given
        self.family = family
        self.birth_date = birth_date
        self.gender = gender
        self.location = location
        self.relationship_status = relationship_status
        self.primary_language = primary_language
        self.contact_phone = contact_phone
        self.driver_license = driver_license
        self.passport_number = passport_number
        self.medical_record_number = medical_record_number
        self.social_security_number = social_security_number
        self.mothers_maiden_name = mothers_maiden_name
        self.race = race
        self.ethnicity = ethnicity

    def to_row(self):
        return Row(
            patient_id=self.patient_id,
            given=self.given,
            family=self.family,
            birth_date=self.birth_date,
            gender=self.gender,
            location=self.location,
            relationship_status=self.relationship_status,
            primary_language=self.primary_language,
            contact_phone=self.contact_phone,
            driver_license=self.driver_license,
            passport_number=self.passport_number,
            medical_record_number=self.medical_record_number,
            social_security_number=self.social_security_number,
            mothers_maiden_name=self.mothers_maiden_name,
            race=self.race,
            ethnicity=self.ethnicity
        )

# Convert RDD[Class B] to Spark DataFrame
df_class_b = rdd_class_b.map(lambda x: x.to_row()).toDF()

# Show the result
display(df_class_b)


# COMMAND ----------


