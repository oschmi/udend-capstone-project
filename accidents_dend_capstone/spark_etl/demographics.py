import os
import click
from pyspark.sql import functions as f, SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.1") \
        .getOrCreate()
    return spark


def process_demographics(spark, input_data, output_data):
    """
    Process demographics data
    Extract transform and load the demographic dataset into an optimized data lake on S3.
    Args:
        spark: SparkSession to use
        input_data: basepath path, from where to read the dataset as csv.
                    Spark looks for an demographics subdir in the given basedir.
        output_data: output basepath. Spark saves the optimized lake in the given basedir under `cities/*.parquet`
    """
    # get filepath to city data file
    demographics_data = os.path.join(input_data, 'demographics/*.json')

    # read demographic file
    df = spark.read.json(demographics_data)
    df = df.select("fields.*")
    df.printSchema()

    # we omit everything race related
    df = df.drop_duplicates(["city", "state_code"])

    # extract columns to create city table
    city_table = df.select(
        f.monotonically_increasing_id().alias("city_id"),
        f.col("city").alias("city_name"),
        "state",
        "state_code",
        "total_population",
        "average_household_size",
        "median_age",
        "number_of_veterans"
    )

    city_table.write.parquet(os.path.join(output_data, 'cities'), 'overwrite')


@click.command()
@click.option("--in", "in_",
              default=None,
              help="Input file of demographic data as *.json")
@click.option("--out",
              default=None,
              help="output dir to store partitioned city files")
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

    process_demographics(spark, in_, out)


if __name__ == "__main__":
    main()
