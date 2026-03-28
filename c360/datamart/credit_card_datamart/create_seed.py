from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
spark = SparkSession.builder.appName("CreditCardData").getOrCreate()

def run(env_config, system, table, operation, timestamp, start_time, end_time, dry_run):
    target_table = env_config.TABLES.get(table, table)
    target_catalog = env_config.CATALOG
    target_schema = env_config.SCHEMA
    source_table = "credit_card_account"
    source_catalog = "gdp_prod"
    source_schema = "customer"

    logging.info(f"Environment: {env_config.ENVIRONMENT}")
    logging.info(f"Target: {target_catalog}.{target_schema}.{target_table}")
    logging.info(f"Operation: {operation}, Dry run: {dry_run}")
    logging.info(f"Window: {start_time} to {end_time}, Timestamp: {timestamp}")

    query = f"""
    INSERT INTO {target_catalog}.{target_schema}.{target_table} (account_number, update_time, identifier_type)
    SELECT account_number, gdp_processtime_stamp, identifier_type
    FROM {source_catalog}.{source_schema}.{source_table}
    WHERE gdp_processtime_stamp > (SELECT MAX(update_time) FROM {target_catalog}.{target_schema}.{target_table})
    or card_type = 'Barclays'
    """

    if dry_run:
        logging.info("Dry run mode - query not executed.")
        logging.info(query)
    else:
        logging.info("Executing query...")
        spark.sql(query)
        logging.info("Insert completed successfully.")