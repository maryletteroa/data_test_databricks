# Databricks notebook source
# %pip install pandas lxml great_expectations pandas_profiling git+https://github.com/julioasotodv/spark-df-profiling.git#egg=spark-df-profiling 

# COMMAND ----------

# MAGIC %run ./paths

# COMMAND ----------

# dbutils.fs.rm(root_dir,True)

# COMMAND ----------

# dbutils.fs.rm(data_dir, True)
# dbutils.fs.rm(data_profile_dir, True)
# dbutils.fs.rm(data_docs_dir, True)

# COMMAND ----------

dbutils.fs.mkdirs(data_dir)
dbutils.fs.mkdirs(data_profile_dir)
dbutils.fs.mkdirs(expectations_suite_dir)
dbutils.fs.mkdirs(validations_dir)
dbutils.fs.mkdirs(data_docs_dir)

# COMMAND ----------

from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
from great_expectations.data_context import BaseDataContext

project_config = DataContextConfig(
    config_version=2,
    plugins_directory=None,
    config_variables_file_path=None,
    datasources={
        "source_dir": {
          "batch_kwargs_generators": {
            "subdir_reader": {
              "class_name": "SubdirReaderBatchKwargsGenerator",
              "base_directory": f"/dbfs/{source_data_dir}"
            }
          },
            "data_asset_type": {
                "class_name": "PandasDataset",
                "module_name": "great_expectations.dataset",
            },
            "class_name": "PandasDatasource",
            "module_name": "great_expectations.datasource",
            "batch_kwargs_generators": {},
        },
        "raw_dir": {
          "batch_kwargs_generators": {
            "subdir_reader": {
              "class_name": "SubdirReaderBatchKwargsGenerator",
              "base_directory": f"/dbfs/{raw_data_dir}"
            }
          },
            "data_asset_type": {
                "class_name": "SparkDFDataset",
                "module_name": "great_expectations.dataset",
            },
            "class_name": "SparkDFDatasource",
            "module_name": "great_expectations.datasource",
            "batch_kwargs_generators": {},
        },
        "clean_dir": {
          "batch_kwargs_generators": {
            "subdir_reader": {
              "class_name": "SubdirReaderBatchKwargsGenerator",
              "base_directory": f"/dbfs/{clean_data_dir}"
            }
          },
            "data_asset_type": {
                "class_name": "SparkDFDataset",
                "module_name": "great_expectations.dataset",
            },
            "class_name": "SparkDFDatasource",
            "module_name": "great_expectations.datasource",
            "batch_kwargs_generators": {},
        },
        "present_dir": {
          "batch_kwargs_generators": {
            "subdir_reader": {
              "class_name": "SubdirReaderBatchKwargsGenerator",
              "base_directory": f"/dbfs/{present_data_dir}"
            }
          },
            "data_asset_type": {
                "class_name": "SparkDFDataset",
                "module_name": "great_expectations.dataset",
            },
            "class_name": "SparkDFDatasource",
            "module_name": "great_expectations.datasource",
            "batch_kwargs_generators": {},
        },
    },
    stores={
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": f"/dbfs/{expectations_suite_dir}",
            },
        },
        "validations_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": f"/dbfs/{validations_dir}",
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    expectations_store_name="expectations_store",
    validations_store_name="validations_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    data_docs_sites={
        "local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": f"/dbfs/{data_docs_dir}",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
        }
    },
    validation_operators={
        "action_list_operator": {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        }
    },
    anonymous_usage_statistics={
      "enabled": True
    }
)

context = BaseDataContext(project_config=project_config)
