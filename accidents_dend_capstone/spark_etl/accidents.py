import os

import click
from pyspark.sql import functions as f

import os

from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.1") \
        .getOrCreate()
    return spark


def process_accidents(spark, input_data, output_data):
    """
    Process accident data
    Extract transform and load the accident dataset into an optimized data lake on S3.
    Args:
        spark: SparkSession to use
        input_data: basepath path, from where to read the dataset as csv.
                    Spark looks for an accidents subdir in the given basedir.
        output_data: output basepath. Spark saves the optimized lake in the given dir.
                    Spark expects a optimized city data in `cities/*.parquet`
    """
    # get filepath to accident data file
    accident_data = os.path.join(input_data, "accidents/*.csv")

    # read accident data files
    df = spark.read.csv(accident_data, header=True)
    df.show()

    # extract relevant columns
    df = df["ID", "Severity", "Start_Time", "End_Time", "Distance(mi)", "Description", "City", "State", "Timezone",
            "Airport_Code", "Weather_Timestamp", "Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)",
            "Wind_Direction", "Wind_Speed(mph)", "Precipitation(in)", "Weather_Condition"]

    weather_conditions_table = extract_weather(df, output_data)
    weather_conditions_table.show(5)

    # read in data to use for city table
    city_df = spark.read.parquet(os.path.join(output_data, "cities/*.parquet"))
    tmp_join_df = df.join(city_df, (df.City == city_df.city_name) & (df.State == city_df.state_code), how="inner")

    tmp_join_df = tmp_join_df.join(weather_conditions_table,
                                   (tmp_join_df.Weather_Condition == weather_conditions_table.condition)
                                   & (tmp_join_df.Wind_Direction == weather_conditions_table.wind_direction)
                                   & (tmp_join_df.Airport_Code == weather_conditions_table.airport_code),
                                   how="left")

    city_df.unpersist()

    # convert string timestamp to datetime
    tmp_join_df = tmp_join_df.withColumn("start_time", f.to_date(f.col("Start_Time")))
    tmp_join_df = tmp_join_df.withColumn("end_time", f.to_date(f.col("End_Time")))

    # extract columns to create accidents table
    accident_table = tmp_join_df.select(
        f.col('ID').alias('accident_id'),
        f.year('start_time').alias('year'),
        f.month('start_time').alias('month'),
        'start_time',
        f.col("Timezone").alias("timezone"),
        f.col('Severity').alias('severity'),
        f.col('Distance(mi)').alias('distance'),
        f.col('Description').alias('description'),
        f.col('Temperature(F)').alias('temperature'),
        f.col('Wind_Speed(mph)').alias('wind_speed'),
        f.col("Humidity(%)").alias("humidity"),
        f.col("Pressure(in)").alias("pressure"),
        f.col("Visibility(mi)").alias("visibility"),
        f.col("Precipitation(in)").alias("precipitation"),
        "city_id",
        "weather_condition_id"
    )

    accident_table.show(5)
    accident_table.write.partitionBy(["year", "month"]).parquet(os.path.join(output_data, "accidents"), "overwrite")


def extract_weather(df, output_data):
    # extract weather_conditions table
    weather_conditions_table = df.select(
        f.monotonically_increasing_id().alias("weather_condition_id"),
        f.col("Weather_Condition").alias("condition"),
        f.col("Wind_Direction").alias("wind_direction"),
        f.col("Airport_Code").alias("airport_code")
    )
    # leave only unique conditions
    weather_conditions_table = weather_conditions_table.dropDuplicates(["condition", "wind_direction", "airport_code"])
    # save weather_conditions table
    weather_conditions_table.write.parquet(os.path.join(output_data, "weather_conditions"), "overwrite")
    return weather_conditions_table


@click.command()
@click.option("--in", "in_",
              default=None,
              help="Input file of us-accidents as *.csv")
@click.option("--out",
              default=None,
              help="output dir to store partitioned accidents and weather files")
@click.option("--emr-mode/--local-mode", "emr_mode",
              default=False,
              help="")
@click.option("--with-aws", "with_aws",
              default=False,
              help="")
def main(in_, out, emr_mode, with_aws):
    # local mode
    if not emr_mode:
        from accidents_dend_capstone.utils.local_settings import load_local_config
        in_, out = load_local_config(with_aws=with_aws)

    spark = create_spark_session()

    process_accidents(spark, in_, out)


if __name__ == "__main__":
    main()
