# Databricks notebook source
# MAGIC %run ./_includes/setup

# COMMAND ----------

# MAGIC %run ./_includes/paths

# COMMAND ----------

# MAGIC %run ./_includes/src

# COMMAND ----------

# MAGIC %md extract

# COMMAND ----------

from glob import glob
import os

if not os.path.exists(f"/dbfs/{source_data_dir}"):
    dbutils.fs.mkdirs(f"/dbfs/{source_data_dir}")

for name, url in data_urls.items():
    print(f"getting data for: {name}...")
    write_table_csv(
        table = get_data_from_urls(url),
        output_dir = f"/dbfs/{source_data_dir}",
        prefix = name
    )

# COMMAND ----------

# MAGIC %md ingest

# COMMAND ----------

names = ("stores", "sales", "features")

if not os.path.exists(f"/dbfs/{raw_data_dir}"):
    os.mkdir(f"/dbfs/{raw_data_dir}")


for name in names:
    df = read_csv_to_spark(
        csv_file_path = f"{source_data_dir}/{name}.csv",
        tag="new"
    )

    print(f"writing raw table for {name}")

    write_spark_table(
        data = df,
        partition_col = "p_ingest_date",
        output_dir = raw_data_dir,
        name = name,
        mode = "overwrite",
    )


# COMMAND ----------

# MAGIC %md transform

# COMMAND ----------

if not os.path.exists(f"/dbfs/{clean_data_dir}"):
    os.mkdir(f"/dbfs/{clean_data_dir}")

print("writing clean stores table...")
write_spark_table(
        data = transform_stores(
            path=f"{raw_data_dir}/stores",
            tag="good"
        ),
        partition_col = "p_ingest_date",
        output_dir = clean_data_dir,
        name = "stores",
        mode = "append",
    )

print("writing clean features table...")
write_spark_table(
        data = transform_features(
            path=f"{raw_data_dir}/features",
            tag="good"
        ),
        partition_col = "p_ingest_date",
        output_dir = clean_data_dir,
        name = "features",
        mode = "append",
    )


#----------- with health check ----- #

sales_tables = tag_negative_sales(
    data = transform_sales(
        path=f"{raw_data_dir}/sales",
        tag="good"
    ),
    tag = "quarantined",
    )

print("writing clean stores sales...")
write_spark_table(
        data = sales_tables.good,
        partition_col = "p_ingest_date",
        output_dir = clean_data_dir,
        name = "sales",
        mode = "append",
    )


sales_tables.quarantined = negative_sales_to_null(
    data = sales_tables.quarantined,
    )

print("taking action on quarantined sales table...")
write_spark_table(
        data = sales_tables.quarantined,
        partition_col = "p_ingest_date",
        output_dir = clean_data_dir,
        name = "sales",
        mode = "append",
    )


# COMMAND ----------

# MAGIC %md present

# COMMAND ----------

if not os.path.exists(f"/dbfs/{present_data_dir}"):
    os.mkdir(f"/dbfs/{present_data_dir}")


@dataclass
class Data:
	stores: pd.DataFrame
	sales:pd.DataFrame
	features: pd.DataFrame


data = Data(
	stores = spark.read.format("parquet").load(f"{clean_data_dir}/stores"),
	sales = spark.read.format("parquet").load(f"{clean_data_dir}/sales"),
	features = spark.read.format("parquet").load(f"{clean_data_dir}/features"),
)


print("writing presentation table for sales department ...")
write_spark_table(
    data = generate_sales_department_data(
        stores_table = data.stores,
        sales_table = data.sales
        ),
    partition_col = "p_ingest_date",
    output_dir = present_data_dir,
    name = "sales_dept",
    mode = "append"
)


print("writing presentation table for data science department ...")
write_spark_table(
    data = generate_ds_department_data(
        stores_table = data.stores,
        sales_table = data.sales,
        features_table = data.features,
        ),
    partition_col = "p_ingest_date",
    output_dir = present_data_dir,
    name = "ds_dept",
    mode = "append"
)


# COMMAND ----------

# MAGIC %md profile data

# COMMAND ----------

if not os.path.exists(f"/dbfs/{source_data_profile_dir}"):
    os.mkdir(f"/dbfs/{source_data_profile_dir}")

for csv in glob(f"/dbfs/{source_data_dir}/*.csv"):
    basename = os.path.basename(csv)
    name = os.path.splitext(basename)[0]
    title = name.title()

    profile = generate_data_profile_from_csv(
        df=pd.read_csv(csv),
        title=title,
        output_dir=f"/dbfs/{source_data_profile_dir}",
        prefix=name,
    )


# COMMAND ----------

if not os.path.exists(f"/dbfs/{raw_data_profile_dir}"):
    os.mkdir(f"/dbfs/{raw_data_profile_dir}")

for table in glob(f"/dbfs/{raw_data_dir}/*"):
    table_ = table.replace("/dbfs/","")
    name = os.path.basename(table)
    data = spark.read.format("parquet").load(table_)

    print(f"building profile for raw table: {name} ...")
    generate_data_profile_from_spark(
        data = data,
        output_dir = f"/dbfs/{raw_data_profile_dir}" ,
        prefix = name
    )


# COMMAND ----------

if not os.path.exists(f"/dbfs/{clean_data_profile_dir}"):
    os.mkdir(f"/dbfs/{clean_data_profile_dir}")

for table in glob(f"/dbfs/{clean_data_dir}/*"):
    table_ = table.replace("/dbfs/","")
    name = os.path.basename(table)
    data = spark.read.format("parquet").load(table_)

    print(f"building profile for raw table: {name} ...")
    generate_data_profile_from_spark(
        data = data,
        output_dir = f"/dbfs/{clean_data_profile_dir}" ,
        prefix = name
    )


# COMMAND ----------

if not os.path.exists(f"/dbfs/{present_data_profile_dir}"):
    os.mkdir(f"/dbfs/{present_data_profile_dir}")

for table in glob(f"/dbfs/{present_data_dir}/*"):
    table_ = table.replace("/dbfs/","")
    name = os.path.basename(table)
    data = spark.read.format("parquet").load(table_)

    print(f"building profile for raw table: {name} ...")
    generate_data_profile_from_spark(
        data = data,
        output_dir = f"/dbfs/{present_data_profile_dir}" ,
        prefix = name
    )


# COMMAND ----------

# MAGIC %md check profiles

# COMMAND ----------

with open("/dbfs/FileStore/data_test_demo/docs/data_profiles/2_clean/stores_data_profile_report.html") as html:
  displayHTML(html.read())

# COMMAND ----------

# MAGIC %md validate

# COMMAND ----------

names = ("stores", "sales", "features")
for name in names:
    validate_csv_file(
        context = context,
        expectation_suite_name = f"source_{name}_expectation_suite",
        path_to_csv = f"/dbfs/{source_data_dir}/{name}.csv",
        datasource_name = "source_dir",
        data_asset_name = f"{name}.csv"
    )


# COMMAND ----------

names = ("stores", "sales", "features")
for name in names:
    validate_spark_table(
        context = context,
        expectation_suite_name = f"raw_{name}_expectation_suite",
        path_to_spark_table = f"{raw_data_dir}/{name}",
        datasource_name = "raw_dir",
        data_asset_name = f"raw_{name}"
    )


# COMMAND ----------

names = ("stores", "sales", "features")
for name in names:
    validate_spark_table(
        context = context,
        expectation_suite_name = f"clean_{name}_expectation_suite",
        path_to_spark_table = f"{clean_data_dir}/{name}",
        datasource_name = "clean_dir",
        data_asset_name = f"clean_{name}"
    )



# COMMAND ----------

names = ("sales_dept", "ds_dept")
for name in names:
    validate_spark_table(
        context = context,
        expectation_suite_name = f"present_{name}_expectation_suite",
        path_to_spark_table = f"{present_data_dir}/{name}",
        datasource_name = "present_dir",
        data_asset_name = f"present_{name}"
    )


# COMMAND ----------

# MAGIC %md check validations

# COMMAND ----------

with open("/dbfs/FileStore/data_test_demo/docs/great_expectations/data_docs/index.html") as html:
  displayHTML(html.read())
