import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

def init_spark():
    """
    Initializer/Getter for a spark session
    """
    
    # Environment variables
    spark_uri = os.environ.get('SPARK_MASTER_URL')

    mongo_user = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
    mongo_pass = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")
    mongo_host = os.environ.get("MONGO_HOST")
    mongo_port = os.environ.get("MONGO_PORT")

    mongodb_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/"
    
    conf = SparkConf()
    conf.setMaster(spark_uri).setAppName("DAE Spark")
    conf.set("spark.submit.deployMode", "client")
    conf.set("spark.driver.bindAddress", "0.0.0.0")
    conf.set("spark.driver.host", "airflow-worker")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.mongodb.read.connection.uri", mongodb_uri)
    conf.set("spark.mongodb.write.connection.uri", mongodb_uri)
    conf.set("spark.jars.packages", "org.postgresql:postgresql:42.7.2,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
    sc = SparkContext(conf=conf)
    ss = SparkSession(sc).builder.getOrCreate()


    return ss