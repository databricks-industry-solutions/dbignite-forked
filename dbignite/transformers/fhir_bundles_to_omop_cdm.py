import json
from copy import deepcopy

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


PERSON_TABLE = 'person'
CONDITION_TABLE = 'condition'
PROCEDURE_OCCURRENCE_TABLE = 'procedure_occurrence'

ENCOUNTER_TABLE = 'encounter'


ENTRY_SCHEMA = StructType([
  StructField('resource', StructType([
    StructField('id', StringType()),
    # Not named in the spec.
    StructField('resourceType', StringType()) 
  ])),
  StructField('request', StructType([
    # Not technically required, but named in spec.
    StructField('url',  StringType())
  ]))
])


def fhir_bundles_to_omop_cdm(path: str, cdm_database: str, mapping_database: str ) -> OmopCdm:
  entries_df = (
    spark.read.text(path, wholetext=True)
    .select(explode(_entry_json_strings('value')).alias('entry_json'))
    .withColumn('entry', from_json('entry_json', schema=ENTRY_SCHEMA))
  ).cache()

  person_df = _entries_to_person(entries_df)
  condition_df = _entries_to_condition(entries_df)
  procedure_occurrence_df = _entries_to_procedure_occurrence(entries_df)
  
  encounter_df = _entries_to_encounter(entries_df)
  
#   cdm_database = f'cdm_{DATABASE_NAME}'
#   mapping_database = f'cdm_mapping_{DATABASE_NAME}'
  spark.sql(f'CREATE DATABASE IF NOT EXISTS {cdm_database}')
  spark.sql(f'CREATE DATABASE IF NOT EXISTS {mapping_database}')
  spark.catalog.setCurrentDatabase(cdm_database)
  
  person_df.writeTo(PERSON_TABLE).createOrReplace()
  condition_df.writeTo(CONDITION_TABLE).createOrReplace()
  procedure_occurrence_df.writeTo(PROCEDURE_OCCURRENCE_TABLE).createOrReplace()
  
  encounter_df.writeTo(ENCOUNTER_TABLE).createOrReplace()
  
  return OmopCdm(cdm_database, mapping_database)


@udf(ArrayType(StringType()))
def _entry_json_strings(value):
  '''
  UDF takes raw text, returns the
  parsed struct and raw JSON.
  '''
  bundle_json = json.loads(value)
  return [json.dumps(e) for e in bundle_json['entry']]


def _entries_to_person(entries_df):
  entry_schema = deepcopy(ENTRY_SCHEMA)
  patient_schema = next(f.dataType for f in entry_schema.fields if f.name == 'resource')
  patient_schema.fields.extend([
    StructField('name', StringType()),
    StructField('gender', StringType()),
    StructField('birthDate', DateType()),
    StructField('address', StringType()),
    StructField('extension', ArrayType(StructType([
      StructField('url', StringType())
    ])))
  ])
  return (
    entries_df
    .where(col('entry.request.url') == 'Patient')
    .withColumn('patient', from_json('entry_json', schema=entry_schema)['resource'])
    .select(
      col('patient.id').alias('person_id'),
      col('patient.name').alias('name'),
      col('patient.gender').alias('gender_source_value'),
      year(col('patient.birthDate')).alias('year_of_birth'),
      month(col('patient.birthDate')).alias('month_of_birth'),
      dayofmonth(col('patient.birthDate')).alias('day_of_birth'),
      col('patient.address').alias('address'),
    )
  )


def _entries_to_condition(entries_df):
  entry_schema = deepcopy(ENTRY_SCHEMA)
  condition_schema = next(f.dataType for f in entry_schema.fields if f.name == 'resource')
  condition_schema.fields.extend([
    StructField('subject', StructType([
      StructField('reference', StringType())
    ])),
    StructField('encounter', StructType([
      StructField('reference', StringType())
    ])),
    StructField('code', StructType([
      StructField('coding', ArrayType(StructType([
        StructField('code', StringType()),
        StructField('display', StringType()),
      ]))),
      StructField('text', StringType())
    ])),
    StructField('onsetDateTime', TimestampType()),
    StructField('abatementDateTime', TimestampType()),
  ])
  return (
    entries_df
    .where(col('entry.request.url') == 'Condition')
    .withColumn('condition', from_json('entry_json', schema=entry_schema)['resource'])
    .select(
      col('condition.id').alias('condition_occurrence_id'),
      regexp_replace(col('condition.subject.reference'),'urn:uuid:','').alias('person_id'),
      regexp_replace(col('condition.encounter.reference'),'urn:uuid:','').alias('visit_occurrence_id'),
      col('condition.onsetDateTime').alias('condition_start_datetime'),
      col('condition.abatementDateTime').alias('condition_end_datetime'),
      col('condition.code.coding')[0]['display'].alias('condition_status'),
      col('condition.code.coding')[0]['code'].alias('condition_code'),
    )
  )


def _entries_to_procedure_occurrence(entries_df):
  entry_schema = deepcopy(ENTRY_SCHEMA)
  procedure_occurrence_schema = next(f.dataType for f in entry_schema.fields if f.name == 'resource')
  procedure_occurrence_schema.fields.extend([

    StructField('subject', StructType([
      StructField('reference', StringType())
    ])),

    StructField('encounter', StructType([
      StructField('reference', StringType())
    ])),

    StructField('code', StructType([
      StructField('coding', ArrayType(StructType([
        StructField('code', StringType()),
        StructField('display', StringType()),
        StructField('system', StringType())
      ]))),
      StructField('text', StringType())
    ])),

    StructField('performedPeriod', StructType([
      StructField('start', TimestampType()),
      StructField('end', TimestampType())
    ])),

    StructField('category', StringType()),
    StructField('performer', StringType()),

    StructField('location', StructType([
      StructField('reference', StringType()),
      StructField('display', StringType()),
    ])),

  ])
  return(
    entries_df
    .where(col('entry.request.url')=="Procedure")
    .withColumn('procedure_occurrence', from_json('entry_json', schema=entry_schema)['resource'])
    .select(
            col('procedure_occurrence.id').alias('procedure_occurrence_id'),
            regexp_replace(col('procedure_occurrence.subject.reference'),'urn:uuid:','').alias('person_id'),
            col('procedure_occurrence.code.coding')[0]['code'].alias('procedure_code'),
            col('procedure_occurrence.code.coding')[0]['display'].alias('procedure_code_display'),
            col('procedure_occurrence.code.coding')[0]['system'].alias('procedure_code_system'),
            col('procedure_occurrence.performedPeriod.start').cast('date').alias('procedure_start_date'),
            col('procedure_occurrence.performedPeriod.end').cast('date').alias('procedure_end_date'),
            col('procedure_occurrence.category').alias('procedure_type'),
            col('procedure_occurrence.performer').alias('provider_id'),
            regexp_replace(col('procedure_occurrence.encounter.reference'),'urn:uuid:','').alias('visit_occurrence_id'),
            col('procedure_occurrence.location.reference').alias('location_id'),
            col('procedure_occurrence.location.display').alias('location_display')
           )
        )
  
def _entries_to_encounter(entries_df):
  
  entry_schema = deepcopy(ENTRY_SCHEMA)
  encounter_schema = next(f.dataType for f in entry_schema.fields if f.name == 'resource')
  
  encounter_schema.fields.extend([

    StructField('subject', StructType([
      StructField('reference', StringType())
    ])),  
    
    StructField('period', StructType([
      StructField('start', TimestampType()),
      StructField('end', TimestampType())
    ])),
    
    
    StructField('serviceProvider', StructType([
      StructField('reference', StringType()),
      StructField('display', StringType())
    ])),
    
   
    StructField('type', ArrayType(StructType([
      StructField('coding', ArrayType(StructType([
        StructField('code', StringType()),
        StructField('display', StringType()),
        StructField('system', StringType())
      ]))),
      StructField('text', StringType())
    ]))),
    
    
    StructField('participant',ArrayType(StructType([                                                                  
      StructField('type', ArrayType(StructType([
      StructField('coding', ArrayType(StructType([
        StructField('code', StringType()),
        StructField('display', StringType()),
        StructField('system', StringType())
      ]))),
      StructField('text', StringType())
    ])))]))),  
  
    StructField('status', StringType()),
    
    StructField('identifier', ArrayType(StructType([
        StructField('use', StringType()),
        StructField('system', StringType()),
        StructField('value', StringType())
      ]))), 
    
    StructField('location', ArrayType(StructType([
        StructField("location",StructType([
        StructField('reference', StringType()),
        StructField('display', StringType())
      ]))]))),  
     

  ])
  
  return (
    entries_df
    .where(col('entry.request.url') == 'Encounter')
    .withColumn('encounter', from_json('entry_json', schema=entry_schema)['resource'])
    .select(
      col('encounter.id').alias('encounter_id'),
      regexp_replace(col('encounter.subject.reference'),'urn:uuid:','').alias('person_id'),
      col('encounter.period.start').alias('encounter_period_start'),
      col('encounter.period.end').alias('encounter_period_end'),
      col('encounter.serviceProvider.display').alias('serviceProvider'),
      col('encounter.type')[0]['coding'][0]['display'].alias('encounter_status'),
      col('encounter.type')[0]['coding'][0]['code'].alias('encounter_code'),
      col('encounter.type')[0]['text'].alias('encounter_status_text'),
      col('encounter.participant'),
      col('encounter.status'),
      col('encounter.identifier'),
      col('encounter.location')

    )
  )