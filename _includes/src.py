# Databricks notebook source
# extract 
import pandas as pd
from typing import IO


def get_data_from_urls(url: str) -> pd.DataFrame:
    """Parses table out from a website
    
    Args:
        url (str): Website URL
    
    Returns:
        pd.DataFrame: A pandas dataframe containing the data
    
    Raises:
        Exception: Catch all for all errors including invalid or unavailable URLs.
    """
    try:
        table: pd.DataFrame = pd.read_html(url, header=1,)[0].iloc[:, 1:]
    except:
        raise Exception("Something went wrong")
    return table


def write_table_csv(table: pd.DataFrame, 
        output_dir: str,
        prefix: str,
        ) -> IO:

    """Writes table to a csv file
    
    Args:
        table (pd.DataFrame): Table or data to be written
        output_dir (str): Output directory
        prefix (str): Name of file
    
    Returns:
        IO: [description]: A csv file
    """
    return table.to_csv(f"{output_dir}/{prefix}.csv", index=False)

# COMMAND ----------

# ingest 
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import (
    current_date,
    current_timestamp, 
    lit)
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame


spark = SparkSession.builder.getOrCreate()

def read_csv_to_spark(
        csv_file_path:str,
        tag: str,
        ) -> pd.DataFrame:
    """
    Reads the csv from source to spark dataframe
    
    Args:
        csv_file_path (str): Path to csv file
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pd.DataFrame: Description
    
    Deleted Parameters:
        spark (SparkSession): Description
    """
    df = spark.read \
        .option("header", True) \
        .csv(csv_file_path)
    df = df \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df

# data checks before writing

def write_spark_table(
        data: pyspark_DataFrame,
        partition_col: str,
        output_dir: str,
        name: str,
        mode: str = "append",
    ) -> None:

    """Writes data to delta table
    
    Args:
        data (pyspark_DataFrame): Spark dataframe
        partition_col (str): Column name where table will be partitioned
        output_dir (str): Output directory
        name (str): Name of table
        mode (str, optional): Mode (default="append")
    
    Returns:
        None: Writes delta table to output_dir
    
    """
    return (
        data \
            .write \
            .format("parquet") \
            .mode(mode) \
            .partitionBy(partition_col)
            .parquet(f"{output_dir}/{name}")
    )

# COMMAND ----------

# transform 
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, 
    current_date, 
    current_timestamp,
    col,
    lit,
    to_date,
    date_format,
    when,
)
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame


spark = SparkSession.builder.getOrCreate()


from dataclasses import dataclass


def transform_stores(
        path: str,
        tag: str,
    ) -> pyspark_DataFrame:
    """
    Transform stores table
    
    Args:
        path (str): Path to stores raw table
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pyspark_DataFrame: Transformed and tagged spark table
    """
    df = spark.read.format("parquet").load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("size", col("size").cast("int")) \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())
    return df


def transform_sales(
    path: str,
    tag: str
    ) -> pyspark_DataFrame:
    """
    Transform stores table
    
    Args:
        path (str): Path to sales raw table
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pyspark_DataFrame: Transformed and tagged spark table
    """
    df = spark.read.format("parquet").load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("dept", col("dept").cast("int")) \
        .withColumn("date", to_date(col("date"), "dd/mm/yyyy")) \
        .withColumn("date", date_format(col("date"), "yyyy-MM-dd")) \
        .withColumn("date", col("date").cast("date")) \
        .withColumn("weekly_sales", col("weekly_sales").cast("float")) \
        .drop("isholiday") \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df

def transform_features(
    path: str,
    tag: str
    ) -> pyspark_DataFrame:
    """
    Transform stores table
    
    Args:
        path (str): Path to features raw table
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pyspark_DataFrame: Transformed and tagged spark table
    """
    df = spark.read.format("parquet").load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("fuel_price", col("fuel_price").cast("float")) \
        .withColumn("date", to_date(col("date"), "dd/mm/yyyy")) \
        .withColumn("date", date_format(col("date"), "yyyy-MM-dd")) \
        .withColumn("date", col("date").cast("date")) \
        .withColumn("unemployment", col("unemployment").cast("float")) \
        .withColumn("temperature", col("temperature").cast("float")) \
        .withColumn("markdown", col("markdown").cast("float")) \
        .withColumn("cpi", col("cpi").cast("float")) \
        .withColumnRenamed("isholiday", "is_holiday") \
        .withColumn("is_holiday", col("is_holiday").cast("boolean")) \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df


# ------------- health check ------------ #


@dataclass
class Sales:

    """
    Contains the partitioned Sales data
    good and quarantined
    """
    
    good: pd.DataFrame
    quarantined: pd.DataFrame



def tag_negative_sales(
    data: pyspark_DataFrame,
    tag: str,
    ) -> Sales:
    """
    
    Adds tags to sales data with negative values
    
    Args:
        data (pyspark_DataFrame): Spark dataframe containing sales table
        tag (str): Tag for quarantined table
    
    Returns:
        Sales: Namedtuple containing the partitioned good and quarantined sales table
    """
    # split data into good and quarantine
    df_quarantine = data.filter(data.weekly_sales < 0)
    df_quarantine = df_quarantine \
        .withColumn("tag", lit(tag)) \


    df_good = data.filter(data.weekly_sales >= 0)


    return Sales(
        good = df_good,
        quarantined= df_quarantine,
        )

def negative_sales_to_null(
    data: pyspark_DataFrame,
    ) -> pyspark_DataFrame:
    """
    
    Updates the values of negative sales to null
    
    Args:
        data (pyspark_DataFrame): Spark datafrane
    
    Returns:
        pyspark_DataFrame: Spark dataframe
    
    """
    data = data \
        .withColumn(
            "weekly_sales", 
            when(col("weekly_sales") < 0, None) \
            .otherwise(col("weekly_sales"))
        ) 

    return data



# COMMAND ----------

# present 
import pandas as pd
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    current_date
)
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame


def generate_sales_department_data(
    stores_table: pyspark_DataFrame,
    sales_table: pyspark_DataFrame,
    ) -> pyspark_DataFrame:
    """
    Args:
        stores_table (pyspark_DataFrame): Spark table containing stores data
        sales_table (pyspark_DataFrame): Spark table containing sales data
    
    Returns:
        pyspark_DataFrame: Spark table for sales department
    """
    df_stores = stores_table \
        .select("store", "type")

    df_sales = sales_table \
        .select("store", "dept", "date", "weekly_sales")

    df = df_sales \
        .join(
            df_stores,
            on=["store"],
            how="left"
        ) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df


# features with sales with stores
def generate_ds_department_data(
    stores_table: pyspark_DataFrame,
    sales_table: pyspark_DataFrame,
    features_table: pyspark_DataFrame,
    ) -> pyspark_DataFrame:
    """
    Args:
        stores_table (pyspark_DataFrame): Spark table containing stores data
        sales_table (pyspark_DataFrame): Spark table containing sales data
        features_table (pyspark_DataFrame): Spark table containing features data
    
    Returns:
        pyspark_DataFrame: Spark table for data science department
    """
    df_stores = stores_table \
        .select("store", "type", "size")

    df_sales = sales_table \
        .select("store", "dept", "weekly_sales")


    df_features = features_table \
        .select("store", "date", "temperature", 
            "fuel_price", "markdown", "cpi", 
            "unemployment", "is_holiday"
        )

    df = df_sales \
        .join(
            df_stores,
            on=["store"],
            how="left"
        ) \
        .join(
            df_features,
            on=["store"],
            how="left"
        ) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())


    return df

# COMMAND ----------

# profile data 

import pandas as pd
from pandas_profiling import ProfileReport
from great_expectations.data_context import DataContext
from great_expectations.dataset import SparkDFDataset
import spark_df_profiling
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame

spark = SparkSession.builder.getOrCreate()


def generate_data_profile_from_csv(
    df: pd.DataFrame, 
    title: str, 
    output_dir: str,
    prefix: str) -> ProfileReport:
    
    """Outputs data pandas data profile from csv
    
    Args:
        df (pd.DataFrame): Pandas dataframe to generate profile from
        title (str): Name of file to go in the report title
        output_dir (str): Root output directory for reports
        prefix (str): Name of report file
    
    Returns:
        ProfileReport: A ProfileReport object
    
    """
    metadata = {
        "description": "This is a sample profiling report.", 
        "creator": "maryletteroa",
        "author": "maryletteroa",
        "url": "www.example.com",
    }

    # >> also column descriptions
    # >> customize profile views
    
    profile = ProfileReport(
        df=df,
        title=f"{title} Data Profile Report",
        minimal=False,
        sensitive=False,
        dataset = metadata,
        explorative=True,
    )
    profile.to_file(f"{output_dir}/{prefix}_data_profile_report.html")

    return profile

def generate_data_profile_from_spark(
    data: pyspark_DataFrame,
    output_dir: str,
    prefix: str
    ) -> None:
    """
    Generates profile reports from Spark tables
    
    Args:
        data (pyspark_DataFrame): Spark Dataframe
        output_dir (str): Output directory of HTML report
        prefix (str): Prefix of HTML file
    
    """
    
    profile = spark_df_profiling.ProfileReport(data)
    profile.to_file(outputfile=f"{output_dir}/{prefix}_data_profile_report.html")


# COMMAND ----------

# build expectation suite 
import pandas as pd
from pandas_profiling import ProfileReport
from great_expectations.data_context import DataContext
from great_expectations.dataset import SparkDFDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame

def build_expectation_suite_from_pandas_profiling(
    pandas_profile: ProfileReport,
    data_context: DataContext,
    suite_name: str,
    )-> None:
    """
    
    Builds GE expectations suite from pandas profiling
    
    Args:
        pandas_profile (ProfileReport): the pandas_profiling object
        data_context (DataContext): GE data context object
        suite_name (str): Suite name
    
    Returns:
        None: Description
    
    """
    return pandas_profile.to_expectation_suite(
        suite_name=suite_name,
        data_context=data_context,
        run_validation=False,
        build_data_docs=True,
    )


def build_expectation_suite_from_spark(
    data: pyspark_DataFrame,
    expectations_path: str
    ) -> None:
    """
    
    Generations expectation suits from Spark dataframe
    
    Args:
        data (pyspark_DataFrame): A Spark dataframe object
        expectations_path (str): Where the expectation suite json file will be written
    """
    
    expectation_suite, validation_result = \
        BasicDatasetProfiler.profile(
            data_asset=SparkDFDataset(data), 
        )

    with open(expectations_path, "w") as outf:
        print(expectation_suite, file=outf)


# COMMAND ----------

# validations


import datetime
import great_expectations
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)


def validate_csv_file(
    context: great_expectations.data_context.DataContext,
    expectation_suite_name: str,
    path_to_csv: str,
    datasource_name: str,
    data_asset_name: str,
    ) -> None:
    """
    
    Function run validation of csv files against GE expectation suites
    
    Args:
        context (great_expectations.data_context.DataContext): GE context
        expectation_suite_name (str): Expectation suite name
        path_to_csv (str): Path to csv file
        datasource_name (str): Datasource name as registered in GE
        data_asset_name (str): Data asset name
    """
    suite = context.get_expectation_suite(expectation_suite_name)
    suite.expectations = []

    batch_kwargs = {
        "path": path_to_csv,
        "datasource": datasource_name,
        "reader_method": "read_csv",
        "data_asset_name": data_asset_name,
    }
    batch = context.get_batch(batch_kwargs, suite)


    results = LegacyCheckpoint(
        name="_temp_checkpoint",
        data_context=context,
        batches=[
            {
              "batch_kwargs": batch_kwargs,
              "expectation_suite_names": [expectation_suite_name]
            }
        ]
    ).run()
    validation_result_identifier = results.list_validation_result_identifiers()[0]
    context.build_data_docs()


def validate_spark_table(
    context: great_expectations.data_context.DataContext,
    expectation_suite_name: str,
    path_to_spark_table: str,
    datasource_name: str,
    data_asset_name: str,
    ) -> None:


    """
    
    Function run validation of spark table against GE expectation suites
    
    Args:
        context (great_expectations.data_context.DataContext): GE context
        expectation_suite_name (str): Expectation suite name
        path_to_spark_table (str): Path to csv file
        datasource_name (str): Datasource name as registered in GE
        data_asset_name (str): Data asset name
    
    """
    suite = context.get_expectation_suite(expectation_suite_name)
    suite.expectations = []

    batch_kwargs = {
        "path": path_to_spark_table,
        "datasource": datasource_name,
        "reader_method": "parquet",
        "data_asset_name": data_asset_name,
    }

    batch = context.get_batch(batch_kwargs, suite)


    results = LegacyCheckpoint(
        name="_temp_checkpoint",
        data_context=context,
        batches=[
            {
              "batch_kwargs": batch_kwargs,
              "expectation_suite_names": [expectation_suite_name]
            }
        ]
    ).run()
    validation_result_identifier = results.list_validation_result_identifiers()[0]
    context.build_data_docs()

