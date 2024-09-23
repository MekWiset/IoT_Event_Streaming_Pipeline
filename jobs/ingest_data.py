from pyspark.sql import SparkSession
from typing import List
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

from plugins.kafkastream_processor import KafkaStreamProcessor
from plugins.utils.configparser import ConfigParser

conf = ConfigParser('config.yaml')
logging.basicConfig(format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)


def stream_to_s3(data_list: List[str]) -> None:

    '''
    Streams data from Kafka topics and uploads it to AWS S3.

    This function performs the following steps:
        1. Initializes a Spark session with the necessary configurations.
        2. Reads data from specified Kafka topics.
        3. Writes the data to AWS S3 in Parquet format, using a checkpointing mechanism.

    Args:
        data_list (List[str]): List of Kafka topic names to read from.

    Raises:
        Exception: Logs errors for Spark session setup or data processing failures.
    '''

    # Spark setup
    try:
        spark = SparkSession.builder.appName('SmartCityStreaming') \
            .config('spark.jars.packages',
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,'
                    'org.apache.hadoop:hadoop-aws:3.3.1,'
                    'com.amazonaws:aws-java-sdk:1.11.469') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .config('spark.hadoop.fs.s3a.access.key', os.getenv('AWS_ACCESS_KEY')) \
            .config('spark.hadoop.fs.s3a.secret.key', os.getenv('AWS_SECRET_KEY')) \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .getOrCreate()
        
        ksp = KafkaStreamProcessor(spark=spark)
        spark.sparkContext.setLogLevel('WARN')

        logging.info('Spark session created successfully.')

    except Exception as e:
        logging.critical(f'Failed to set up Spark session: {e}')
        return

    # Upload data to AWS S3
    for data in data_list:
        try:
            schema = conf.get_schema(data)
            df = ksp.read_topic(data, schema)
            checkpointFolder = conf.get_storage_path(data, 'checkpoint_folder_path')
            output = conf.get_storage_path(data, 'path')

            ksp.s3_writer(input=df, checkpointFolder=checkpointFolder, output=output)

            logging.info(f'The {data} has been uploaded to S3 successfully.')

        except Exception as e:
            logging.error(f'Failed to process {data}: {e}')


if __name__ == '__main__':

    dotenv_path = Path('.env')
    load_dotenv(dotenv_path = dotenv_path)

    try:
        data_list = conf.get_data_name()
        stream_to_s3(data_list=data_list)
    except Exception as e:
        logging.critical(f'An error occurred in the main execution: {e}')