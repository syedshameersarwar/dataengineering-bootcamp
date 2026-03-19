"""
Batch job: read session aggregates from PostgreSQL and keep the row with the
largest num_trips (longest session by trip count).

Run after session_window_job.py has finished and populated green_trips_session_counts.
"""
from pyflink.table import EnvironmentSettings, TableEnvironment


def create_source_session_counts(t_env: TableEnvironment) -> str:
    table_name = "green_trips_session_counts_src"
    t_env.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            pulocationid INTEGER,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_session_counts',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
        """
    )
    return table_name


def create_sink_longest(t_env: TableEnvironment) -> str:
    table_name = "green_trips_longest_session_sink"
    t_env.execute_sql(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            pulocationid INTEGER,
            num_trips BIGINT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_longest_session',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
        """
    )
    return table_name


def run():
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = TableEnvironment.create(settings)

    src = create_source_session_counts(t_env)
    sink = create_sink_longest(t_env)

    ranked = f"{src}_ranked"
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW {ranked} AS
        SELECT
            window_start,
            window_end,
            pulocationid,
            num_trips,
            ROW_NUMBER() OVER (ORDER BY num_trips DESC) AS rn
        FROM {src}
        """
    )

    t_env.execute_sql(
        f"""
        INSERT INTO {sink}
        SELECT
            1 AS id,
            pulocationid,
            num_trips,
            window_start,
            window_end
        FROM {ranked}
        WHERE rn = 1
        """
    ).wait()


if __name__ == "__main__":
    run()
