from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import yaml
from typing import List, Any, Tuple


class ConfigParser():
    
    '''
    Parses and manages configuration data from a YAML file.

    Args:
        config_path (str): The path to the configuration YAML file.

    Methods:
        load_config(self) -> None:
            Loads configuration data from the YAML file.
        
        get_coordinates(self, country: str, city: str) -> dict:
            Retrieves coordinates for the specified country and city.
        
        get_data_name(self) -> List[str]:
            Returns a list of data names defined in the configuration.
        
        get_data_info(self, data: str, info: str) -> Any:
            Retrieves specific information for the given data type.
        
        get_min_max(self, data: str, info: str):
            Returns the minimum and maximum values for the specified data info.
        
        get_schema(self, data: str) -> StructType:
            Retrieves the schema for a given data type from the configuration.
        
        get_bucket_info(self, bucket_name: str) -> Tuple[str, str]:
            Retrieves the name and location of the specified S3 bucket.
        
        get_storage_path(self, data: str, info: str) -> str:
            Retrieves the storage path for the specified data type and info.
    '''
    
    def __init__(self, config_path: str) -> None:
        '''Initialize the ConfigParser with the configuration file path.'''
        self.config = config_path
        self.load_config()

    def load_config(self) -> None:
        '''Loads configuration data from the YAML file.'''
        with open(self.config, 'r') as file:
            self.config_data = yaml.safe_load(file)

    def get_coordinates(self, country: str, city: str) -> dict:
        '''Retrieves coordinates for the specified country and city.'''
        return self.config_data['coordinates'][country][city][0]

    def get_data_name(self) -> List[str]:
        '''Returns a list of data names defined in the configuration.'''
        return list(self.config_data['data'].keys())

    def get_data_info(self, data: str, info: str) -> Any:
        '''Retrieves specific information for the given data type.'''
        return self.config_data['data'][data][info]
    
    def get_min_max(self, data: str, info: str):
        '''Returns the minimum and maximum values for the specified data info.'''
        return (
            self.get_data_info(data=data, info=info)[0]['min'],
            self.get_data_info(data=data, info=info)[0]['max']
        )
    
    def get_schema(self, data: str) -> StructType:
        '''Retrieves the schema for a given file name from the configuration.'''
        schema = StructType()
        for field in self.config_data['data'][data]['schema']:
            schema.add(field['column_name'], eval(field['data_type'])(), field['nullable'])
        return schema
    
    def get_bucket_info(self, bucket_name: str) -> Tuple[str, str]:
        '''Retrieves the name and location of the specified S3 bucket.'''
        name = self.config_data['AWS']['S3']['buckets'][bucket_name]['name']
        location = self.config_data['AWS']['S3']['buckets'][bucket_name]['location']
        return name, location
    
    def get_storage_path(self, data: str, info: str) -> str:
        '''Retrieves the storage path for the specified data type and info.'''
        return self.config_data['AWS']['S3']['storage_paths'][data][info]