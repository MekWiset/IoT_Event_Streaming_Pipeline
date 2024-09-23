import random
from datetime import datetime, timedelta
import uuid
import simplejson as json
from confluent_kafka import SerializingProducer
from typing import Tuple, Any

from plugins.utils.configparser import ConfigParser


class DataGenerator():
    
    '''
    Generates various types of simulated data for a given device.

    Args:
        device_id (str): The unique identifier for the device.

    Methods:
        get_next_timestamp(self, start_time: datetime) -> datetime:
            Returns the next timestamp based on the start time.
        
        generate_gps_data(self, timestamp: str, vehicle_type='private') -> dict:
            Generates GPS data for a vehicle.
        
        generate_traffic_camera_data(self, timestamp: str, location: Tuple[float, float]) -> dict:
            Generates data from a traffic camera at the specified location.
        
        generate_weather_data(self, timestamp: str, location: Tuple[float, float]) -> dict:
            Generates weather data for the specified location.
        
        generate_emergency_incident_data(self, timestamp: str, location: Tuple[float, float]) -> dict:
            Generates data for an emergency incident.
        
        generate_vehicle_data(self, start_location: dict, start_time: datetime, from_coordinate: dict, to_coordinate: dict) -> dict:
            Generates vehicle data based on start location and time.
    '''
    
    def __init__(self, device_id: str) -> None:
        '''Initialize the DataGenerator with the device ID and configuration.'''
        self.device_id = device_id
        self.conf = ConfigParser('config.yaml')
        random.seed(42)

    def get_next_timestamp(self, start_time: datetime) -> datetime:
        '''Returns the next timestamp based on the start time.'''
        next_timestamp = start_time + timedelta(seconds=random.randint(30, 60))
        return next_timestamp
    
    def generate_gps_data(self, timestamp: str, vehicle_type='private') -> dict:
        '''Generates GPS data for a vehicle.'''
        speed_min, speed_max = self.conf.get_min_max(data='gps_data', info='speed')
        return {
            'id': str(uuid.uuid4()),
            'deviceId': self.device_id,
            'timestamp': timestamp,
            'speed': random.uniform(speed_min, speed_max),
            'direction': self.conf.get_data_info(data='gps_data', info='direction'),
            'vehicleType': vehicle_type
        }
    
    def generate_traffic_camera_data(self, timestamp: str, location: Tuple[float, float]) -> dict:
        '''Generates data from a traffic camera at the specified location.'''
        return {
            'id': str(uuid.uuid4()),
            'deviceId': self.device_id,
            'cameraId': self.conf.get_data_info(data='traffic_camera_data', info='camera_id'),
            'location': location,
            'timestamp': timestamp,
            'snapshot': 'Base64EncodedString'
        }
    
    def generate_weather_data(self, timestamp: str, location: Tuple[float, float]) -> dict:
        '''Generates weather data for the specified location.'''
        temperature_min, temperature_max = self.conf.get_min_max('weather_data', 'temperature')
        precipitation_min, precipitation_max = self.conf.get_min_max('weather_data', 'precipitation')
        wind_speed_min, wind_speed_max = self.conf.get_min_max('weather_data', 'wind_speed')
        humidity_min, humidity_max = self.conf.get_min_max('weather_data', 'humidity')
        air_quality_index_min, air_quality_index_max = self.conf.get_min_max('weather_data', 'airquality_index')
        return {
            'id': str(uuid.uuid4()),
            'deviceId': self.device_id,
            'location': location,
            'timestamp': timestamp,
            'temperature': random.uniform(temperature_min, temperature_max),
            'weatherCondition': random.choice(self.conf.get_data_info(data='weather_data', info='weather_condition')),
            'precipitation': random.uniform(precipitation_min, precipitation_max),
            'windSpeed': random.uniform(wind_speed_min, wind_speed_max),
            'humidity': random.randint(humidity_min, humidity_max),
            'airQualityIndex': random.uniform(air_quality_index_min, air_quality_index_max),
        }

    def generate_emergency_incident_data(self, timestamp: str, location: Tuple[float, float]) -> dict:
        '''Generates data for an emergency incident.'''
        return {
            'id': str(uuid.uuid4()),
            'deviceId': self.device_id,
            'incidentId': uuid.uuid4(),
            'type': random.choice(self.conf.get_data_info(data='emergency_data', info='type')),
            'timestamp': timestamp,
            'location': location,
            'status': random.choice(self.conf.get_data_info(data='emergency_data', info='status')),
            'description': 'Description of the incident'
        }
    
    def generate_vehicle_data(self, start_location: dict, start_time: datetime, from_coordinate: dict, to_coordinate: dict) -> dict:
        '''Generates vehicle data based on start location and time.'''
        start_location['latitude'] += (to_coordinate['latitude'] - from_coordinate['latitude']) / 100
        start_location['longitude'] += (to_coordinate['longitude'] - from_coordinate['longitude']) / 100
    
        start_location['latitude'] += random.uniform(-0.0005, 0.0005)
        start_location['longitude'] += random.uniform(-0.0005, 0.0005)

        speed_min, speed_max = self.conf.get_min_max(data='vehicle_data', info='speed')

        return {
            'id': str(uuid.uuid4()),
            'deviceId': self.device_id,
            'timestamp': self.get_next_timestamp(start_time=start_time).isoformat(),
            'location': (start_location['latitude'], start_location['longitude']),
            'speed': random.uniform(speed_min, speed_max),
            'direction': self.conf.get_data_info(data='vehicle_data', info='direction'),
            'make': self.conf.get_data_info(data='vehicle_data', info='make'),
            'model': self.conf.get_data_info(data='vehicle_data', info='model'),
            'year': self.conf.get_data_info(data='vehicle_data', info='year'),
            'fuelType': self.conf.get_data_info(data='vehicle_data', info='fuel_type'),
        }
    

class StreamingSimulator(DataGenerator):

    '''
    Simulates streaming data generation and production to Kafka.

    Args:
        device_id (str): The unique identifier for the device.
        bootstrap_server (str): The address of the Kafka bootstrap server.

    Methods:
        json_serializer(self, obj: Any) -> str:
            Serializes an object to JSON format.
        
        delivery_report(self, err: Exception, msg: Any) -> None:
            Reports the delivery status of a message.
        
        set_producer(self) -> SerializingProducer:
            Configures and returns a Kafka producer.
        
        produce_data_to_kafka(self, topic: str, data: dict) -> None:
            Produces data to the specified Kafka topic.
    '''
    
    def __init__(self, device_id: str, bootstrap_server: str) -> None:
        '''Initialize the StreamingSimulator with device ID and bootstrap server address.'''
        super().__init__(device_id)
        self.bootstrap_server = bootstrap_server
        self.producer = self.set_producer()

    def json_serializer(self, obj: Any) -> str:
        '''Serializes an object to JSON format.'''
        if isinstance(obj, uuid.UUID):
            return str(obj)
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')
    
    def delivery_report(self, err: Exception, msg: Any) -> None:
        '''Reports the delivery status of a message.'''
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def set_producer(self) -> SerializingProducer:
        '''Configures and returns a Kafka producer.'''
        producer_config = {
            'bootstrap.servers': self.bootstrap_server,
            'error_cb': lambda e: print(f'Kafka error: {e}')
        }
        producer = SerializingProducer(producer_config)
        return producer

    def produce_data_to_kafka(self, topic: str, data: dict) -> None:
        '''Produces data to the specified Kafka topic.'''
        self.producer.produce(
            topic,
            key=str(data['id']),
            value=json.dumps(data, default=self.json_serializer).encode('utf-8'),
            on_delivery=self.delivery_report
        )
        self.producer.flush()