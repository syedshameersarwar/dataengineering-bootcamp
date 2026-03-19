"""
1-hour tumbling windows on event_time (lpep_pickup_datetime): SUM(tip_amount) globally.

Uses streaming mode + bounded Kafka (earliest → latest-offset) so the job finishes
after the topic is fully read, matching the session-window job pattern.
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env: StreamTableEnvironment) -> str:
    table_name = "green_trips_src_hourly_tips"
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
            'properties.group.id' = 'flink-green-trips-hourly-tips-stream',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_sink_postgres(t_env: StreamTableEnvironment) -> str:
    table_name = "green_trips_hourly_tip_totals"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_tip_amount DOUBLE,
            PRIMARY KEY (window_start, window_end) NOT ENFORCED
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
    env.enable_checkpointing(60 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    src = create_events_source_kafka(t_env)
    sink = create_sink_postgres(t_env)

    filtered = f"{src}_filtered"
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW {filtered} AS
        SELECT * FROM {src}
        WHERE tip_amount IS NOT NULL
          AND lpep_pickup_datetime IS NOT NULL
        """
    )

    t_env.execute_sql(
        f"""
        INSERT INTO {sink}
        SELECT
            window_start,
            window_end,
            SUM(tip_amount) AS total_tip_amount
        FROM TABLE(
            TUMBLE(
                TABLE {filtered},
                DESCRIPTOR(event_time),
                INTERVAL '1' HOUR
            )
        )
        GROUP BY window_start, window_end
        """
    ).wait()


if __name__ == "__main__":
    run()
