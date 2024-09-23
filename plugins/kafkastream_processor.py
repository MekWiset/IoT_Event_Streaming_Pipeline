from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col


class KafkaStreamProcessor():

    '''
    Processes streaming data from a Kafka topic using Spark.

    Args:
        spark (SparkSession): The Spark session used for processing.

    Methods:
        read_topic(self, topic: str, schema: StructType, starting_offset: str = 'earliest') -> DataFrame:
            Reads data from the specified Kafka topic and returns a DataFrame.
        
        s3_writer(input: DataFrame, checkpointFolder: str, output_location: str) -> None:
            Writes the input DataFrame to S3 in Parquet format with checkpointing.
    '''
    
    def __init__(self, spark: SparkSession) -> None:
        '''Initialize the KafkaStreamProcessor with a Spark session.'''
        self.spark = spark

    def read_topic(self, topic: str, schema: StructType, starting_offset: str = 'earliest') -> DataFrame:
        '''Reads data from the specified Kafka topic and returns a DataFrame.'''
        return (self.spark.readStream.
                format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', starting_offset)
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )
    
    def s3_writer(self, input: DataFrame, checkpointFolder: str, output_location: str) -> None:
        '''Writes the input DataFrame to S3 in Parquet format with checkpointing.'''
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output_location)
                .outputMode('append')
                .start())