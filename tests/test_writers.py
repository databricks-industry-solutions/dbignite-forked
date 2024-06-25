import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 
from .test_base import PysparkBaseTest

class TestReaders(PysparkBaseTest):
    def test_write_ndjson_column(self):
        from dbignite.readers import read_from_directory
        import json

        sample_data = "./sampledata/*json"
        bundle = read_from_directory(sample_data)
        bundle.entry()
        patients = bundle.entry().select(bundle._get_ndjson_resources("Patient"))
        assert patients.count() == 3
        
        p1,p2,p3 = patients.take(3)
        assert type(json.loads(p1.Patient_ndjson)) == type({})
        assert type(json.loads(p2.Patient_ndjson)) == type({})
        assert type(json.loads(p3.Patient_ndjson)) == type({})
        
        data = json.loads(p1.Patient_ndjson)
        assert list(data.keys()) == ['id', 'meta', 'text', 'extension', 'identifier', 'name', 'telecom', 'gender', 'birthDate', 'address', 'maritalStatus', 'multipleBirthBoolean', 'communication']
        assert data.get("name")[0].get("family") == 'Bernhard'
        
    def test_write_ndjson_row(self):
        from dbignite.readers import read_from_directory
        import json

        sample_data = "./sampledata/*json"
        bundle = read_from_directory(sample_data)
        bundle.entry()

if __name__ == '__main__':
    unittest.main()
