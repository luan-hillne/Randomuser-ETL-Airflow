import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import expr

# Create keyspace cassandra
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        full_name TEXT,
        gender TEXT,
        dateofbirth TEXT,
        address TEXT,
        city TEXT,
        latitude TEXT,
        longtitude TEXT,
        email TEXT );
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    full_name = kwargs.get('full_name')
    gender = kwargs.get('gender')
    dateofbirth = kwargs.get('dateofbirth')
    address = kwargs.get('address')
    city = kwargs.get('city')
    email = kwargs.get('email')
    latitude = kwargs.get('latitude')
    longtitude = kwargs.get('longtitude')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, full_name, gender,dateofbirth, address, 
                city, latitude, longtitude, email)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, full_name, gender,dateofbirth, address, 
                city, latitude, longtitude, email))
        logging.info(f"Data inserted for {full_name} {email}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # Subscribe to 1 topic defaults to the earliest and latest offsets
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9091') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # Connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    #  Define the schema for the JSON data
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("dateofbirth", StringType(), False),  # Corrected field name
        StructField("address", StringType(), False),
        StructField("city", StringType(), False),
        StructField("latitude", StringType(), False),
        StructField("longtitude", StringType(), False),  # Corrected field name
        StructField("email", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def start_streaming(df):
    # Starts the streaming to table spark_streaming.random_names in cassandra

    logging.info("Streaming is being started...")
    my_query = (df.writeStream
                  .format("org.apache.spark.sql.cassandra")
                  .outputMode("append")
                  .options(table="random_names", keyspace="spark_streaming")\
                  .start())

    return my_query.awaitTermination()

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    df = connect_to_kafka(spark_conn)
    selection_df = create_selection_df_from_kafka(df)

    authentication = PlainTextAuthProvider(username='cassandra', password='cassandra')
    # Connect to the cluster (update the IP to your Cassandra node)
    cluster = Cluster(['localhost'])

    # Start a session
    session = cluster.connect()

    # Create keyspace
    create_keyspace(session)

    # Create table
    create_table(session)

    start_streaming(selection_df)

    # Close session and cluster connection
    session.shutdown()
    cluster.shutdown()
