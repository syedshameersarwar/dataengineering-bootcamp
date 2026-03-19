"""
Session windows: gap = 5 minutes between pickup timestamps (per PULocationID).
Event time: lpep_pickup_datetime → event_time; watermark 5 seconds behind event_time.

Flink does not support SESSION windows in batch mode (see FLINK error:
"Unaligned windows like session are not supported in batch mode yet").

We use streaming mode + bounded Kafka (earliest → latest-offset snapshot) so the
source stops after the topic is fully read and the job can finish. Session
aggregates emit upserts → JDBC sink needs a primary key.
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env: StreamTableEnvironment) -> str:
    table_name = "green_trips_src_session"
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
            'properties.group.id' = 'flink-green-trips-session-stream',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_sink_postgres(t_env: StreamTableEnvironment) -> str:
    table_name = "green_trips_session_counts"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            pulocationid INTEGER,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, pulocationid) NOT ENFORCED
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
        WHERE PULocationID IS NOT NULL
          AND lpep_pickup_datetime IS NOT NULL
        """
    )

    # SESSION must use PARTITION BY PULocationID inside the TVF. GROUP BY alone does
    # NOT scope sessions per zone — without PARTITION BY, Flink merges ALL trips into
    # global sessions (~400+ trips), which matches a ~422 "max" count.
    t_env.execute_sql(
        f"""
        INSERT INTO {sink}
        SELECT
            window_start,
            window_end,
            PULocationID AS pulocationid,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(
                TABLE {filtered} PARTITION BY PULocationID,
                DESCRIPTOR(event_time),
                INTERVAL '5' MINUTES
            )
        )
        GROUP BY window_start, window_end, PULocationID
        """
    ).wait()


if __name__ == "__main__":
    run()
