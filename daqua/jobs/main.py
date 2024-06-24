import logging

from daqua.utils.db_config import DBConfig
from daqua.utils.pg_spark import PgSparkUtil

logger = logging.getLogger(__name__)


class DQUtils:
    def compare_schemas(self, src, target):
        src_schema = src.schema
        target_schema = target.schema

        # Convert schemas to sets of (name, type, nullable) for comparison
        src_fields = set(
            (field.name, str(field.dataType), field.nullable) for field in src_schema
        )
        target_fields = set(
            (field.name, str(field.dataType), field.nullable) for field in target_schema
        )

        # Identify differences
        only_in_source = src_fields - target_fields
        only_in_target = target_fields - src_fields

        if only_in_source or only_in_target:
            logger.error("Schema mismatch")
            logger.error(f"Fields only in source: {only_in_source}", )
            logger.error(f"Fields only in target: {only_in_target}", )
            return False
        else:
            logger.info("Schema matches")
            return True

    def compare_row_count(self, src, target):
        src_count = src.count()
        target_count = target.count()

        if src_count != target_count:
            logger.error("Row count mismatch")
            logger.error(f"Source count: {src_count}", )
            logger.error(f"Target count:{target_count}", )
            return False
        else:
            logger.info("Row count matches")
            return True


def main():
    appName = "PostgresCompare"
    jar_path = "postgresql-42.7.3.jar"

    # Setup Source
    src_db_config = DBConfig(
        host="localhost",
        port="5432",
        username="world",
        password="world123",
        database="world-db",
    )
    src_pg_spark = PgSparkUtil(appName, jar_path, src_db_config)
    src_table_name = "country"
    src_df = src_pg_spark.read_table_data(src_table_name)

    # Setup Target
    target_db_config = DBConfig(
        host="localhost",
        port="5432",
        username="world",
        password="world123",
        database="world-db",
    )
    target_pg_spark = PgSparkUtil(appName, jar_path, target_db_config)
    target_table_name = "city"
    target_df = target_pg_spark.read_table_data(target_table_name)

    dq_utils = DQUtils()

    logger.info(f"Comparing {src_table_name} and {target_table_name}")

    if dq_utils.compare_schemas(src_df, target_df):
        logger.info("schema is the same")
    if dq_utils.compare_row_count(src_df, target_df):
        logger.info("row count is the same")
