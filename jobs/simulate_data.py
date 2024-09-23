import os
import time
from datetime import datetime
import logging

from plugins.streaming_simulator import StreamingSimulator
from plugins.utils.configparser import ConfigParser

conf = ConfigParser('config.yaml')
logging.basicConfig(format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)


def simulate_data(origin: dict, destination: dict, start_time: datetime = datetime.now()) -> None:

    '''
    Simulates the generation of vehicle-related data and produces it to Kafka until the vehicle reaches its destination.

    This function performs the following steps:
        1. Generates vehicle data based on the starting location and time.
        2. Generates additional data types (GPS, traffic camera, weather, emergency).
        3. Produces the generated data to Kafka topics.
        4. Checks if the vehicle has reached its destination to terminate the simulation.

    Args:
        origin (dict): The starting coordinates of the vehicle.
        destination (dict): The destination coordinates of the vehicle.
        start_time (datetime, optional): The time when the simulation starts. Defaults to the current time.

    Raises:
        Exception: Logs an error if data generation or production to Kafka fails.
    '''

    while True:

        # Generate data
        try:
            vehicle_data = streamsim.generate_vehicle_data(start_location=origin, start_time=start_time, from_coordinate=origin, to_coordinate=destination)
            timestamp = vehicle_data['timestamp']
            location = vehicle_data['location']

            try:
                gps_data = streamsim.generate_gps_data(timestamp=timestamp)
            except Exception as e:
                logging.error(f'Failed to generate GPS data: {e}')
                gps_data = None

            try:
                traffic_camera_data = streamsim.generate_traffic_camera_data(timestamp=timestamp, location=location)
            except Exception as e:
                logging.error(f'Failed to generate traffic camera data: {e}')
                gps_data = None
            
            try:
                weather_data = streamsim.generate_weather_data(timestamp=timestamp, location=location)
            except Exception as e:
                logging.error(f'Failed to generate weather data: {e}')
                weather_data = None

            try:
                emergency_data = streamsim.generate_emergency_incident_data(timestamp=timestamp, location=location)
            except Exception as e:
                logging.error(f'Failed to generate emergency data: {e}')
                emergency_data = None

        except Exception as e:
            logging.error(f'An error occurred during data generation: {e}')

        # Add a condition of termination
        if (vehicle_data['location'][0] >=  destination['latitude']) and (vehicle_data['location'][1] <= destination['longitude']):
            logging.info('Vehicle has reached its destination. Simulation ending...')
            break

        # Produce data to Kafka Topic
        data_to_produce = [vehicle_data, gps_data, traffic_camera_data, weather_data, emergency_data]
        for data in data_to_produce:
            if data is not None:
                try:
                    streamsim.produce_data_to_kafka(topic=str(data), data=data)
                except Exception as e:
                    logging.warning(f'Could not produce {data} to Kafka topic: {e}.\nSkipping {data}...')

        time.sleep(5)


if __name__=='__main__':

    try:
        streamsim = StreamingSimulator(device_id='Vehicle-123', bootstrap_server=os.getenv('KAFKA_BOOTSTRAP_SERVERS'))

        origin = conf.get_coordinates(country='England', city='London')
        destination = conf.get_coordinates(country='England', city='Birmingham')

        simulate_data(origin, destination)
    except Exception as e:
        logging.critical(f'An error in the main execution: {e}')