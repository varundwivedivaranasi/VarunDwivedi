# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List, Dict
import json

# COMMAND ----------

input_path = "/Volumes/workspace/myschema/myvol/source/resturant_json_data.json"
reject_path = "/Volumes/workspace/myschema/myvol/reject"
rules_path = "/Volumes/workspace/myschema/myvol/validation_rules/quality_rules.json"

# COMMAND ----------

#readh source file into dataframe
df_source = spark.read.option("multiline", "true").json(input_path)

#read validation rule file 
with open(rules_path, "r") as f:
    quality_rules = json.load(f)

# COMMAND ----------

df = df_source.withColumn("restaurants",explode("restaurants"))\
    .withColumn("id",col("restaurants.restaurant.id"))\
        .withColumn("restaurant name",col("restaurants.restaurant.name"))\
            .withColumn("cuisines",col("restaurants.restaurant.cuisines"))\
                .withColumn("ratings",col("restaurants.restaurant.user_rating.rating_text"))\
                    .withColumn("city",col("restaurants.restaurant.location.city"))\
                        .withColumn("establishment_types",explode_outer(col("restaurants.restaurant.establishment_types")))\
                            .withColumn("deeplink",col("restaurants.restaurant.deeplink"))\
                                .drop("code","message","results_found","results_shown","results_start","status","restaurants")
                                
                                

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, trim, current_timestamp
from typing import List, Dict
import uuid

class DataQualityValidator:
    def __init__(self, df: DataFrame, notebook_name: str):
        self.df = df
        self.notebook_name = notebook_name
        self.timestamp = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
        self.results = []

    def _generate_rule_id(self):
        return str(uuid.uuid4())

    def check_not_null_or_blank(self, column_name: str, severity: str):
        rule_id = self._generate_rule_id()
        failed_df = self.df.filter(
            col(column_name).isNull() | (trim(col(column_name)) == "")
        ).withColumn("error", lit(f"{column_name} is null or blank")) \
         .withColumn("rule_id", lit(rule_id)) \
         .withColumn("severity", lit(severity)) \
         .withColumn("notebook_name", lit(self.notebook_name)) \
         .withColumn("timestamp", lit(self.timestamp))

        self.results.append({
            "rule_id": rule_id,
            "rule": f"{column_name} should not be null or blank",
            "severity": severity,
            "failed_count": failed_df.count(),
            "failed_records": failed_df
        })

    def check_pattern(self, column_name: str, pattern: str, severity: str):
        rule_id = self._generate_rule_id()
        failed_df = self.df.filter(~col(column_name).rlike(pattern)) \
            .withColumn("error", lit(f"{column_name} does not match pattern '{pattern}'")) \
            .withColumn("rule_id", lit(rule_id)) \
            .withColumn("severity", lit(severity)) \
            .withColumn("notebook_name", lit(self.notebook_name)) \
            .withColumn("timestamp", lit(self.timestamp))

        self.results.append({
            "rule_id": rule_id,
            "rule": f"{column_name} should match pattern '{pattern}'",
            "severity": severity,
            "failed_count": failed_df.count(),
            "failed_records": failed_df
        })

    def run_checks(self, rules: List[Dict[str, any]]):
        for rule in rules:
            if rule["type"] == "not_null_or_blank":
                self.check_not_null_or_blank(rule["column"], rule["severity"])
            elif rule["type"] == "pattern":
                self.check_pattern(rule["column"], rule["pattern"], rule["severity"])

    def show_results(self):
        for res in self.results:
            print(f"[{res['severity'].upper()}] Rule: {res['rule']} | Failed Count: {res['failed_count']}")
            res['failed_records'].show(truncate=False)

    def get_summary_df(self) -> DataFrame:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        summary_data = [(r["rule_id"], r["rule"], r["severity"], r["failed_count"],
                         self.notebook_name, self.timestamp) for r in self.results]

        return spark.createDataFrame(summary_data, schema=[
            "rule_id", "rule", "severity", "failed_count", "notebook_name", "timestamp"
        ])

    def log_errors_to_blob(self, path: str):
        for res in self.results:
            res["failed_records"].write.mode("append").json(path)

# COMMAND ----------

validator = DataQualityValidator(df, notebook_name="Restaurant_Metadata_Validation")
validator.run_checks(quality_rules)
#validator.show_results()

summary_df = validator.get_summary_df()
summary_df.display()

validator.log_errors_to_blob(reject_path)
