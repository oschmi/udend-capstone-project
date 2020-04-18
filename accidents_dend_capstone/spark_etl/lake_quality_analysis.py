import click
from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.1") \
        .getOrCreate()
    return spark


def analyze_quality(spark: SparkSession, input_path, expected_min_count: int):
    """
    Check the row count of a given dataset (needs to be in parquet format)
    Args:
        spark: the spark session to use
        input_path: input dir
        expected_min_count: min row count expected
    """
    df = spark.read.parquet(input_path)
    df.printSchema()
    df.show(5)
    df_count = df.count()

    assert df_count >= expected_min_count


@click.command()
@click.option("--in", "in_",
              default=None,
              help="Input dir with .parquet files to analyze")
@click.option("--min-count", "min_count",
              default=1,
              help="minimum expected row count")
@click.option("--emr-mode/--local-mode", "emr_mode",
              default=False,
              help="")
@click.option("--with-aws", "with_aws",
              default=False,
              help="")
def main(in_, min_count, emr_mode, with_aws):
    # local mode
    if not emr_mode:
        from accidents_dend_capstone.utils.local_settings import load_local_config
        in__, _ = load_local_config(with_aws=with_aws)
        in_ = in_ or in__

    spark = create_spark_session()

    analyze_quality(spark, in_, min_count)


if __name__ == "__main__":
    main()
