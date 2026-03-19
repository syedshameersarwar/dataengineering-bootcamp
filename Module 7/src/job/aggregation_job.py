from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env: StreamTableEnvironment) -> str:
    table_name = "green_trips_src"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            event_time AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-green-trips-windowed-batch',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_sink_postgres(t_env: StreamTableEnvironment) -> str:
    # Column names must match PostgreSQL physical columns. Unquoted PG identifiers
    # are lowercased, so use pulocationid here to match a typical PG table DDL.
    # Batch mode + bounded Kafka → insert-only changelog; no PK required on sink DDL.
    table_name = "green_trips_5min_counts"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            pulocationid INTEGER,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    try:
        from pyflink.common import RuntimeExecutionMode

        env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    except Exception:
        # Batch table mode still applies via EnvironmentSettings below.
        pass

    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    src = create_events_source_kafka(t_env)
    sink = create_sink_postgres(t_env)

    filtered = f"{src}_filtered"
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW {filtered} AS
        SELECT * FROM {src}
        WHERE PULocationID IS NOT NULL
          AND lpep_pickup_datetime IS NOT NULL
        """
    )

    t_env.execute_sql(
        f"""
        INSERT INTO {sink}
        SELECT
            window_start,
            PULocationID AS pulocationid,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(
                TABLE {filtered},
                DESCRIPTOR(event_time),
                INTERVAL '5' MINUTES
            )
        )
        GROUP BY window_start, PULocationID
        """
    ).wait()


if __name__ == "__main__":
    run()