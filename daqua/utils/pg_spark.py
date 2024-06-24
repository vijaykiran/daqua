from daqua.utils.db_config import DBConfig
from pyspark.sql import SparkSession


class PgSparkUtil:
    def __init__(self, appName, jar_path, db_config: DBConfig):
        self.db_config = db_config
        self.spark = (
            SparkSession.builder.appName(appName)
            .config("spark.jars", jar_path)
            .getOrCreate()
        )

    def read_table_data(self, table_name):
        table_data_df = self.spark.read.jdbc(
            table=table_name,
            url=self.db_config.jdbc_url,
            properties=self.db_config.connection_properties,
        )
        return table_data_df

    def read_query_data(self, query):
        query_data_df = self.spark.read.jdbc(
            url=self.db_config.jdbc_url,
            table=query,
            properties=self.db_config.connection_properties,
        )
        return query_data_df
