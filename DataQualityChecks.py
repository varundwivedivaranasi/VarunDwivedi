# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List, Dict

# COMMAND ----------

input_path = "/Volumes/workspace/myschema/myvol/source/resturant_json_data.json"
reject_path = "/Volumes/workspace/myschema/myvol/reject"

# COMMAND ----------

df_source = spark.read.option("multiline", "true").json(input_path)

# COMMAND ----------

df = df_source.withColumn("restaurants",explode("restaurants"))\
    .withColumn("id",col("restaurants.restaurant.id"))\
        .withColumn("restaurant name",col("restaurants.restaurant.name"))\
            .withColumn("cuisines",col("restaurants.restaurant.cuisines"))\
                .withColumn("ratings",col("restaurants.restaurant.user_rating.rating_text"))\
                    .withColumn("city",col("restaurants.restaurant.location.city"))\
                        .withColumn("establishment_types",explode_outer(col("restaurants.restaurant.establishment_types")))\
                            .drop("code","message","results_found","results_shown","results_start","status","restaurants")
                                
                                

# COMMAND ----------

df.display()

# COMMAND ----------

def check_not_null(df, column_name: str) -> Dict[str, any]:
    failed_df = df.filter(col(column_name).isNull())
    result = {
        "rule": f"{column_name} should not be null",
        "failed_count": failed_df.count(),
        "failed_records": failed_df.withColumn("error", lit(f"{column_name} is null"))
    }
    return result

# COMMAND ----------

def run_quality_checks(df, checks: List[Dict[str, any]]) -> List[Dict[str, any]]:
    results = []
    for check in checks:
        if check["type"] == "not_null":
            result = check_not_null(df, check["column"])
            results.append(result)
    return results

# COMMAND ----------

quality_rules = [
    {"type": "not_null", "column": "cuisines"}
]

results = run_quality_checks(df, quality_rules)

# Show failed records
for res in results:
    print(f"Rule: {res['rule']}, Failed Count: {res['failed_count']}")
    res['failed_records'].show(truncate=False)

# COMMAND ----------

def log_errors_to_blob(failed_df, path: str):
    failed_df.write.mode("append").json(path)

# Example
log_errors_to_blob(results[0]["failed_records"], reject_path)
