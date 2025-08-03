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
                            .withColumn("deeplink",col("restaurants.restaurant.deeplink"))\
                                .drop("code","message","results_found","results_shown","results_start","status","restaurants")
                                
                                

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col, lit, trim
from typing import List, Dict

# Rule 1: Null or Blank Check
def check_not_null_or_blank(df, column_name: str) -> Dict[str, any]:
    failed_df = df.filter(
        col(column_name).isNull() | (trim(col(column_name)) == "")
    )
    
    result = {
        "rule": f"{column_name} should not be null or blank",
        "failed_count": failed_df.count(),
        "failed_records": failed_df.withColumn(
            "error", lit(f"{column_name} is null or blank")
        )
    }
    
    return result

# Rule 2: Pattern Match Check
def check_pattern(df, column_name: str, pattern: str) -> Dict[str, any]:
    failed_df = df.filter(~col(column_name).rlike(pattern))
    
    result = {
        "rule": f"{column_name} should match pattern '{pattern}'",
        "failed_count": failed_df.count(),
        "failed_records": failed_df.withColumn(
            "error", lit(f"{column_name} does not match expected pattern")
        )
    }
    
    return result

# COMMAND ----------

# Unified Quality Check Runner
def run_quality_checks(df, checks: List[Dict[str, any]]) -> List[Dict[str, any]]:
    results = []
    for check in checks:
        column = check["column"]
        if check["type"] == "not_null_or_blank":
            result = check_not_null_or_blank(df, column)
        elif check["type"] == "pattern":
            result = check_pattern(df, column, check["pattern"])
        else:
            continue  # Unknown rule type
        results.append(result)
    return results

# COMMAND ----------

# Sample Rules
quality_rules = [
    {"type": "not_null_or_blank", "column": "cuisines"},
    {"type": "pattern", "column": "deeplink", "pattern": r"^zomato://restaurant/\d+$"}
]

# Run Checks
results = run_quality_checks(df, quality_rules)

# Show Failed Records
for res in results:
    print(f"Rule: {res['rule']}, Failed Count: {res['failed_count']}")
    res['failed_records'].show(truncate=False)

# COMMAND ----------

# Logging to Blob
def log_errors_to_blob(failed_df, path: str):
    failed_df.write.mode("append").json(path)

# Example
if results:
    log_errors_to_blob(results[0]["failed_records"], reject_path)
