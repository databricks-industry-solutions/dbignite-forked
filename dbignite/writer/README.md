# Writing FHIR Examples

## Source Data

Dataframes are the source for converting to FHIR. This can be a single table or a transformed table from a SQL Statement

```python
#Read in a DataFrame to convert to FHIR. Data is publicly downloaded from CMS SynPUF

from dbignite.writer.fhir_encoder import *
from dbignite.writer.bundler import *
import json

data = spark.sql("""
select 
--Patient info
b.DESYNPUF_ID, --Patient.id
b.BENE_BIRTH_DT, --Patient.birthDate
b.BENE_COUNTY_CD, --Patient.address.postalCode
c.CLM_ID,  --Claim.id
c.HCPCS_CD_1, --Claim.procedure.procedureCodeableConcept.coding.code
c.HCPCS_CD_2, --Claim.procedure.procedureCodeableConcept.coding.code
c.ICD9_DGNS_CD_1, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
c.ICD9_DGNS_CD_2, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
"http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets" as hcpcs_cdset
from hls_healthcare.hls_cms_synpuf.ben_sum b 
    inner join hls_healthcare.hls_cms_synpuf.car_claims c 
        on c.DESYNPUF_ID = b.DESYNPUF_ID
                 """)
```

## How do transformations happen without writing code? 

Transformations are sourced from a set shown [here](https://github.com/databrickslabs/dbignite/blob/main/dbignite/writer/fhir_encoder.py#L155-L173). This is map of source data type to target data type and the explicit lambda function that performs the action

e.g. an array of strings is mapped to a string by the default behavior of [concatening arrays with a comma](https://github.com/databrickslabs/dbignite/blob/main/dbignite/writer/fhir_encoder.py#L165-L167)
```python
FhirEncoder(False, False, lambda x: ','.join(x))
```
## Mapping from Source to FHIR Specification

```python
maps = [Mapping('DESYNPUF_ID', 'Patient.id'), 
		Mapping('BENE_BIRTH_DT', 'Patient.birthDate'),
		Mapping('BENE_COUNTY_CD', 'Patient.address.postalCode'),
    Mapping('CLM_ID', 'Claim.id'),
    Mapping('HCPCS_CD_1', 'Claim.procedure.procedureCodeableConcept.coding.code'),
    Mapping('HCPCS_CD_2', 'Claim.procedure.procedureCodeableConcept.coding.code'),
    #hardcoded values for system of HCPCS
    Mapping('ICD9_DGNS_CD_1', 'Claim.diagnosis.diagnosisCodeableConcept.coding.code'),
    Mapping('ICD9_DGNS_CD_2', 'Claim.diagnosis.diagnosisCodeableConcept.coding.code')
  ]
```

## Specifying transformations

This default transformations can be overriden or more data types can be added. Here we default strings to concatenate by newline characters instead of commas
```python
em = FhirEncoderManager()
em.DEFAULT_ENCODERS['array<string>']['string'] = FhirEncoder(False, False, lambda x: '\n'.join(x))
```

## Custom Transformations

Transformations can be customized based upon the target column type. Here we map multiple procedure codes into a string (comma separated list by default) and map each procedure code into the "code" element along with the assocaited "system" coding of HCPCS.

```python
em = FhirEncoderManager(
  override_encoders ={
    "Claim.procedure.procedureCodeableConcept.coding": 
      FhirEncoder(False, False, lambda x:
        [{"code": y,
          "system": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets"} 
            for y in x[0].get("code").split(",")])
  })
```

## Complex Extensions

Extensions typically involve mapping multiple elements into an array together. The challenge is that in addition to mapping into an array, elements in the array are typically sub grouped together as an object in an array.

e.g. "code1" + "system1" + "code2" + "system2" -> [{"code1", "system1"}, {"code2", "system2"}]

This can be done in either the Dataframe creation step or 

```python
TODO DataFrame creation
```

```python
TODO custom lambda function
```

## Using different versions of FHIR

```python
em = FhirEncoderManager(..., fhir_schema = FhirSchemaModel(schema_version="r4")
```

# Common Errors

## Encoder doesn't exist yet

## Encoder data type doesn't match expected value
