import configparser
from pyspark import SparkConf




def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("/Users/shyamsundarreddysureddy/Desktop/sales_project/configs/application.conf")
    print("Sections found:", config.sections())
    app_conf = {}
    for key, value in config.items(env):
        app_conf[key] = value
        return app_conf
    



def get_pyspark_config(env):
    config=configparser.ConfigParser()
    config.read("/Users/shyamsundarreddysureddy/Desktop/sales_project/configs/pyspark.conf")
    spark_conf=SparkConf()
    for (key,value) in  config.items(env):
        spark_conf.set(key,value)
    return spark_conf




get_app_config("Local")
