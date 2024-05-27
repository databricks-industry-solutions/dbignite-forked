# Databricks notebook source
# MAGIC %md ### Version 3 (Latest)

# COMMAND ----------

from typing import Any, Type, Callable, Dict

class Converter:
    """
    A class for converting values between different types.
    """
    
    DIRECT_MAPPINGS = {"int", "float", "str", "bool"}
    LOGICAL_MAPPINGS = {
        ("str", "list"): lambda x, *args, **kwargs: x.split(*args, **kwargs),
        ("list", "str"): lambda x, *args, **kwargs: " ".join(str(item) for item in x),
    }

    def __new__(cls, value: Any, target_type: Type, *args, **kwargs) -> Any:
        """
        Converts the value to the specified target type.

        Parameters:
            value (Any): The value to be converted.
            target_type (Type): The target type to convert the value to.
 
        Returns:
            Any: The converted value in the target type.
        """
        
        source_type_name = type(value).__name__
        target_type_name = target_type.__name__

        if source_type_name == target_type_name:
            return value
        elif {source_type_name, target_type_name}.issubset(cls.DIRECT_MAPPINGS):
            return target_type(value)
        else:
            conversion_key = (source_type_name, target_type_name)
            if conversion_key in cls.LOGICAL_MAPPINGS:
                return cls.LOGICAL_MAPPINGS[conversion_key](value, *args, **kwargs)
            else:
                raise ValueError(f"Conversion from {source_type_name} to {target_type_name} is not supported.")

# COMMAND ----------

s1 = Converter("5", int)                                   
s2 = Converter("5.4", float)                                
s3 = Converter("one,two,three", list, sep=',', maxsplit=1)  
s4 = Converter(5, str)                                
s5 = Converter(5, float)
s6 = Converter(5.4, float)
s7 = Converter(5.4, int)
s8 = Converter(["one", "two", "three"], str)

# COMMAND ----------

# MAGIC %md ### Version 2

# COMMAND ----------

class TypeConverter:
    """
    A class for converting values between different types.
    """
    def __new__(cls, value: Any, target_type: Type, *args, **kwargs) -> Any:
        """
        Converts the value to the specified target type.

        Parameters:
            value (Any): The value to be converted.
            target_type (Type): The target type to convert the value to.
 
        Returns:
            Any: The converted value in the target type.
        """
        MAPPINGS = {
            ("str", "int"): int,
            ("str", "float"): float,
            ("str", "bool"): bool,
            ("str", "list"): lambda x: x.split(*args, **kwargs),
            ("int", "str"): str,
            ("int", "float"): float,
            ("int", "bool"): bool,
            ("float", "str"): str,
            ("float", "int"): int,
            ("float", "bool"): bool,
            ("bool", "str"): str,
            ("bool", "int"): int,
            ("bool", "float"): float,
            ("list", "str"): lambda x: " ".join(str(x_) for x_ in x),
        }
        
        source_type_name = type(value).__name__
        target_type_name = target_type.__name__
        
        if source_type_name == target_type_name:
            return value
        else:
            return MAPPINGS[(source_type_name, target_type_name)](value)

# COMMAND ----------

# MAGIC %md ### Version 1

# COMMAND ----------


from typing import Any, Dict, Type, TypeVar

class CustomDecoder():
    T = TypeVar('T')

    def __init__(self, source_object: Any, target_class: Type[T]) -> None:
        """
        Initialize the decoder with a source object and the target class.
        
        Args:
            source_object: The object to be decoded.
            target_class: The class to which the object should be converted.
        """
        self.source_object = source_object
        self.target_class = target_class

    def decode(self) -> T:
        """
        Decode the source object to an instance of the target class.
        
        Returns:
            An instance of the target class with attributes populated from the source object.
        """
        decoded_object = {}

        source_attrs = self.source_object.__dict__
        target_attrs = self.target_class.__init__.__annotations__

        # assuming that classes have the same number of attributes
        for source_value, (target_attr, target_type) in zip(source_attrs.values(), target_attrs.items()):
            decoded_attr = CustomDecoder.any_to_any(source_value, target_type)
            decoded_object[target_attr] = decoded_attr

        return self.target_class(**decoded_object)

    @staticmethod
    def any_to_any(source_value: Any, target_type: Type) -> Any:
        """
        Convert a source value to the target type.
        
        Args:
            source_value: The value to be converted.
            target_type: The type to which the value should be converted.
        
        Returns:
            The converted value.
        """        
        if isinstance(source_value, target_type):
            decoded_attr = source_value
        elif isinstance(source_value, str):
            decoded_attr = CustomDecoder.str_to_any(source_value, target_type)
        elif target_type == str:
            decoded_attr = CustomDecoder.any_to_str(source_value)            
        ### add more conversions
        else:
            raise NotImplementedError
        return decoded_attr
    
    @staticmethod
    def str_to_any(value: str, target_type: Type) -> Any:
        """
        Convert a string value to the target type.
        
        Args:
            value: The string value to be converted.
            target_type: The type to which the value should be converted.
        
        Returns:
            The converted value.
        """
        if target_type == int:
            converted_value = int(value)
        elif target_type == float:
            converted_value = float(value)
        elif target_type == bool:
            converted_value = bool(value)
        elif target_type == list:
            converted_value = value.split(" ")
        else: 
            converted_value = value
        return converted_value
    
    @staticmethod
    def any_to_str(value: Any) -> str:
        """
        Convert any value to a string.
        
        Args:
            value: The value to be converted.
        
        Returns:
            The converted string.
        """

        if isinstance(value, list):
            converted_value = " ".join(str(item) for item in value)
        else:
            converted_value = str(value)
        return converted_value

# COMMAND ----------

class myHospitalPatient:
    def __init__(self, name: str, surname: str, age: int, salary: str):
        self.name = name
        self.surname = surname
        self.age = age
        self.salary = salary
    
    def __repr__(self):
        return f"myHospitalPatient(name={self.name}, surname={self.surname}, age={self.age}, salary={self.salary})"
    
class InternalPatient:
    def __init__(self, given_name: list, lastname: str, age: int, my_salary: float):
        self.given_name = given_name
        self.lastname = lastname
        self.age = age
        self.my_salary = my_salary

    def __repr__(self):
        return f"InternalPatient(given_name={self.given_name}, lastname={self.lastname}, age={self.age}, my_salary={self.my_salary})"  

# COMMAND ----------

internal_patient_obj = InternalPatient(given_name=["FirstName","SecondName"], lastname="LastName", age=23, my_salary=1200.8)
d = CustomDecoder(internal_patient_obj, myHospitalPatient)
d.decode()

# COMMAND ----------

myHospitalPatient_obj = myHospitalPatient(name="FirstName SecondName", surname="Surname", age=23, salary="1300.7")
d = CustomDecoder(myHospitalPatient_obj, InternalPatient)
print(d.decode())
print(type(d.decode().my_salary))

# COMMAND ----------


