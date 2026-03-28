import sys
import importlib
from datamart.credit_card_datamart import create_seed  # Changed to relative import assuming module is in parent dir

def load_env_config(env):
    return importlib.import_module(f"config.{env}.config")

def main(args):
    env, system, table, operation, timestamp, start_time, end_time, dry_run = args
    dry_run = dry_run in ["True", "true", "1"] if isinstance(dry_run, str) else dry_run

    env_config = load_env_config(env)

    if operation == "create_seed":
        create_seed.run(env_config, system, table, operation, timestamp, start_time, end_time, dry_run)
    else:
        raise ValueError(f"Unsupported operation: {operation}")

if __name__ == "__main__":
    main(sys.argv[1:])

def insert_audit_start(env_config, system, table, operation, start_time):
    audit_table = f"{env_config.CATALOG}.{env_config.SCHEMA}.{env_config.AUDIT_TABLE}"
    spark.sql(f"""
        INSERT INTO {audit_table} VALUES (
            '{system}', '{system}', '{table}', '{operation}',
            TIMESTAMP('{start_time}'), NULL, NULL
        )
    """)

def update_audit_end(env_config, system, table, operation, start_time, end_time, success):
    audit_table = f"{env_config.CATALOG}.{env_config.SCHEMA}.{env_config.AUDIT_TABLE}"
    spark.sql(f"""
        UPDATE {audit_table}
        SET end_time = TIMESTAMP('{end_time}'), success = {str(success).upper()}
        WHERE system = '{system}' AND table_name = '{table}' AND operation = '{operation}'
          AND start_time = TIMESTAMP('{start_time}')
    """)


def main(args):
    env, system, table, operation, timestamp, start_time, end_time, dry_run = args
    dry_run = dry_run in ["True", "true", "1"] if isinstance(dry_run, str) else dry_run

    env_config = load_env_config(env)

    try:
        # Insert audit record at job start
        insert_audit_start(system, table, operation, start_time)

        if operation == "create_seed":
            create_seed.run(env_config, system, table, operation, timestamp, start_time, end_time, dry_run)

        # Update audit record on success
        update_audit_end(system, table, operation, start_time, end_time, success=True)

    except Exception as e:
        # Update audit record on failure
        update_audit_end(system, table, operation, start_time, end_time, success=False)
        raise
