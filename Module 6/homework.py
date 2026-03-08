import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyspark

    return


@app.cell
def _():
    from pyspark.sql import SparkSession

    return (SparkSession,)


@app.cell
def _(SparkSession):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('homework-yellow-november') \
        .getOrCreate()

    return (spark,)


@app.cell
def _(spark):
    df_yellow = spark.read.parquet('data/yellow_tripdata_2025-11.parquet')
    return (df_yellow,)


@app.cell
def _(df_yellow):
    df_yellow.printSchema()
    return


@app.cell
def _():
    ### Question 2
    return


@app.cell
def _(df_yellow):
    df_yellow.repartition(4).write.parquet("data/processed/")
    return


@app.cell
def _():
    import subprocess

    result = subprocess.run(["ls", "-la", "--block-size=M", "./data/processed/"], capture_output=True, text=True)
    print(result.stdout)
    return


@app.cell
def _():
    ### Question 3
    return


@app.cell
def _(df_yellow):
    df_yellow.show(n=5)
    return


@app.cell
def _(df_yellow):
    from pyspark.sql import functions as F
    count = df_yellow.filter(F.to_date("tpep_pickup_datetime") == "2025-11-15").count()
    count
    return (F,)


@app.cell
def _():
    ### Question 4
    return


@app.cell
def _(F, df_yellow):
    longest_trip = (
        df_yellow.withColumn(
            "diff_hours",
            (F.unix_timestamp("tpep_dropoff_datetime") - 
             F.unix_timestamp("tpep_pickup_datetime")) / 3600
        )
        .orderBy(F.col("diff_hours").desc())
        .limit(1)
    )
    return (longest_trip,)


@app.cell
def _(longest_trip):
    longest_trip.show()
    return


@app.cell
def _():
    ### Question 6
    return


@app.cell
def _(spark):
    zone_lookup = spark.read.option("header", "true").csv("./data/taxi_zone_lookup.csv")
    return (zone_lookup,)


@app.cell
def _(zone_lookup):
    zone_lookup.show()
    return


@app.cell
def _(zone_lookup):
    zone_lookup.createOrReplaceTempView("zone_lookup")
    return


@app.cell
def _(spark):
    spark.sql("""
    SELECT
        *
    FROM
        zone_lookup
    """).show()
    return


@app.cell
def _(df_yellow):
    df_yellow.createOrReplaceTempView("yellow_tripdata")
    return


@app.cell
def _(spark):
    lowest_pickup_zone = spark.sql("""
    WITH pickup_counts AS (
        SELECT 
            z.Zone AS pickup_zone,
            COUNT(*) AS cnt
        FROM yellow_tripdata t
        JOIN zone_lookup z
            ON t.PULocationID = z.LocationID
        GROUP BY z.Zone
    ),
    ranked_pickups AS (
        SELECT
            pickup_zone,
            cnt,
            DENSE_RANK() OVER (ORDER BY cnt ASC) AS rnk
        FROM pickup_counts
    )
    SELECT pickup_zone, cnt
    FROM ranked_pickups
    WHERE rnk = 1;
    """)
    return (lowest_pickup_zone,)


@app.cell
def _(False162604162604, lowest_pickup_zone):
    lowest_pickup_zone.show(truncate = False162604162604)
    return


if __name__ == "__main__":
    app.run()
