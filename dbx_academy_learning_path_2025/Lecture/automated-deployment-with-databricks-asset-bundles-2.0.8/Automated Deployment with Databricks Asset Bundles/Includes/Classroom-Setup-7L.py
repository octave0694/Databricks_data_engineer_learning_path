# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

## Create the DA keys for the user's catalogs
DA.create_DA_keys()

## Display the course catalog and schema name for the user.
DA.display_config_values(
  [
    ('DEV catalog reference: DA.catalog_dev', DA.catalog_dev),
    ('STAGE catalog reference: DA.catalog_stage', DA.catalog_stage),
    ('PROD catalog reference: DA.catalog_prod', DA.catalog_prod)
   ]
)

# COMMAND ----------

!pip install mlflow==2.21.3

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, log, pow, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from pathlib import Path


class MLModelHandler:
    def __init__(self):
        self.catalog_name = f'{DA.catalog_name}'
        self.schema_name = 'default'
        self.username = DA.username
        self.env = '_1_dev'
        self.csv_file_path = f'/Volumes/{DA.catalog_name}_3_prod/default/health/2025-01-02_health.csv'
    
    def run_pipeline(self):
        if self.model_existence_check():
            print("Model already exists in dev and stage. Pipeline did not run.")
            return None
        else: 
            cleaned_df = self.clean_raw_data()
            transformed_df = self.transform_cleaned_data(cleaned_df)
            feature_df, target_df = self.create_features(transformed_df)
            train_data = self.create_training_data(feature_df, target_df)
            self.train_and_register_model(train_data)
            self.env = '_2_stage'
            self.train_and_register_model(train_data)
    
    def clean_raw_data(self):
        raw_data = (
            spark.read
            .option('header', True)
            .option('inferSchema', True)
            .csv(self.csv_file_path)
        )
        numeric_columns = [col_name for col_name in raw_data.columns 
                           if raw_data.select(col_name).schema[0].dataType.typeName() in ['int', 'double']]
        
        for column in numeric_columns:
            mean_value = raw_data.select(mean(col(column)).alias("mean")).first()["mean"]
            raw_data = raw_data.fillna({column: mean_value})
        
        return raw_data.na.drop()
    
    def transform_cleaned_data(self, cleaned_df):
        transformed_df = (
            cleaned_df
            .withColumn("log_BMI", log(col("BMI") + 1))
            .withColumn("log_Age", log(col("Age") + 1))
            .withColumn("BMI_squared", pow(col("BMI"), 2))
            .drop("PII", "date")
            .na.drop()
        )
        return transformed_df
    
    def create_features(self, transformed_df):
        target_df = transformed_df.select("ID", "Diabetes_binary")
        feature_df = transformed_df.drop("Diabetes_binary")
        feature_df = (
            feature_df
            .withColumn("HighBP", when(col("HighBP") == "1.0", 1).otherwise(0))
            .withColumn("Age", col("Age").cast("int"))
        )
        return feature_df, target_df
    
    def create_training_data(self, feature_df, target_df):
        input_cols = [col_name for col_name in feature_df.columns if col_name != "ID"]
        assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
        feature_data = assembler.transform(feature_df)
        sdf = feature_data.join(target_df, on="ID")
        train_data, test_data = sdf.randomSplit([0.8, 0.2], seed=42)
        return train_data
    
    def model_existence_check(self):
        mlflow.set_registry_uri("databricks-uc")
        client = MlflowClient()
        model_env_name = f"diabetes_model_dev"
        registry_model_dev = f"{self.catalog_name}{self.env}.{self.schema_name}.{model_env_name}" # If dev exists, stage will also exist 
        existing_versions = client.search_model_versions(f"name = '{registry_model_dev}'")
        print(f"Checking to see if model exists in {self.catalog_name}{self.env}...")
        if existing_versions:
            latest_ver = max(int(v.version) for v in existing_versions)
            model_uri   = f"models:/{registry_model_dev}/{latest_ver}"
            print(f"Model already exists: {model_uri}")
            return True
        else:
            print(f"No versions found in {self.catalog_name}{self.env}{self.env}; you can train and register a new model.")
            return False

    
    def train_and_register_model(self, train_data):
        # Train & log new model
        model_base = f"{self.catalog_name}{self.env}.{self.schema_name}.diabetes_model_dev"
        print(f"No existing model found; training & registering diabetes_model in {self.catalog_name}{self.env}")
        with mlflow.start_run():
            rf = RandomForestClassifier(featuresCol="features", labelCol="Diabetes_binary")
            model = rf.fit(train_data)
            # infer signature on train→predict
            sig = infer_signature(
                train_data.select("features").toPandas(),
                model.transform(train_data).select("prediction").toPandas()
            )

            # log & register
            mlflow.log_param("environment", self.env)
            mlflow.spark.log_model(
                spark_model = model,
                artifact_path = "model",
                registered_model_name = model_base,
                signature = sig
            )
        print(f"Model registered successfully for environment: {self.catalog_name}{self.env}")

# Run the pipeline
handler = MLModelHandler()
handler.run_pipeline()

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common-Install-CLI
