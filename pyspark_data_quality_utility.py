"""
PySpark Data Quality Utility
Author: ChatGPT

Features:
- Environment parameterization via YAML/JSON config
- Schema validation against provided StructType
- Data quality checks (null %, unique key checks, range checks, regex checks)
- Audit logging to Parquet/JSON (pluggable via abstraction)
- SOLID design: Single Responsibility, Open/Closed, Liskov, Interface Segregation, Dependency Inversion

Usage:
- Provide a config YAML (sample included below) and run as a script

Note: This file is a single-file utility for demonstration. In production prefer packaging into modules and adding unit tests.
"""
from __future__ import annotations

import json
import logging
import re
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

# ---------- Configuration / Environment Parameterization (SRP) ----------

@dataclass
class AppConfig:
    app_name: str
    env: str
    input_path: str
    output_path: str
    audit_path: str
    schema_path: Optional[str]
    checks: List[Dict[str, Any]]
    unique_keys: List[str]
    min_partitions: int = 1

    @staticmethod
    def from_yaml(path: str) -> "AppConfig":
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
        return AppConfig(
            app_name=cfg.get("app_name", "DataQualityApp"),
            env=cfg.get("env", "dev"),
            input_path=cfg["input_path"],
            output_path=cfg["output_path"],
            audit_path=cfg.get("audit_path", "./audit"),
            schema_path=cfg.get("schema_path"),
            checks=cfg.get("checks", []),
            unique_keys=cfg.get("unique_keys", []),
            min_partitions=cfg.get("min_partitions", 1),
        )

# ---------- Abstractions for Audit Logging (DIP + ISP) ----------

class AuditSink(ABC):
    @abstractmethod
    def write(self, record: Dict[str, Any]) -> None:
        pass

class FileAuditSink(AuditSink):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def write(self, record: Dict[str, Any]) -> None:
        # simple append JSON Lines per day
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        filename = f"{self.base_path}/audit_{date_str}.jsonl"
        with open(filename, "a") as f:
            f.write(json.dumps(record) + "\n")

# ---------- Schema Validation (SRP) ----------

class SchemaValidator:
    def __init__(self, spark: SparkSession, schema: Optional[StructType] = None):
        self.spark = spark
        self.schema = schema

    def load_schema_from_json(self, path: str) -> StructType:
        # Expect schema in Spark JSON schema format
        with open(path, "r") as f:
            schema_json = json.load(f)
        self.schema = StructType.fromJson(schema_json)
        return self.schema

    def validate(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """Validate that the DataFrame columns match the expected schema.
        Returns (is_valid, list_of_mismatches)
        """
        if self.schema is None:
            return True, []
        expected_fields = {f.name: f.dataType for f in self.schema.fields}
        actual_fields = {f.name: f.dataType for f in df.schema.fields}
        mismatches = []
        # Check missing and extra fields
        for name in expected_fields:
            if name not in actual_fields:
                mismatches.append(f"Missing column: {name}")
        for name in actual_fields:
            if name not in expected_fields:
                mismatches.append(f"Unexpected column: {name}")
        # Could add type-checking with isinstance
        return (len(mismatches) == 0), mismatches

# ---------- Data Quality Checks (SRP, OCP) ----------

class CheckResult:
    def __init__(self, name: str, passed: bool, details: Dict[str, Any]):
        self.name = name
        self.passed = passed
        self.details = details

class DataQualityChecker:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def null_percentage_check(self, df: DataFrame, column: str, max_percent: float) -> CheckResult:
        total = df.count()
        if total == 0:
            return CheckResult(f"null_pct_{column}", True, {"total": 0})
        nulls = df.filter(F.col(column).isNull()).count()
        pct = (nulls / total) * 100
        passed = pct <= max_percent
        return CheckResult(f"null_pct_{column}", passed, {"column": column, "null_pct": pct, "max_percent": max_percent})

    def duplicate_check(self, df: DataFrame, keys: List[str], max_duplicates: int = 0) -> CheckResult:
        grouped = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1)
        dup_count = grouped.count()
        passed = dup_count <= max_duplicates
        return CheckResult("duplicate_check", passed, {"duplicate_groups_found": dup_count, "keys": keys})

    def regex_check(self, df: DataFrame, column: str, pattern: str, max_failed_pct: float = 0.0) -> CheckResult:
        total = df.count()
        if total == 0:
            return CheckResult(f"regex_{column}", True, {"total": 0})
        bad = df.filter(~F.col(column).rlike(pattern) | F.col(column).isNull()).count()
        pct_failed = (bad / total) * 100
        passed = pct_failed <= max_failed_pct
        return CheckResult(f"regex_{column}", passed, {"column": column, "failed_pct": pct_failed, "pattern": pattern, "max_failed_pct": max_failed_pct})

    def range_check(self, df: DataFrame, column: str, min_value: Optional[float], max_value: Optional[float]) -> CheckResult:
        cond = None
        if min_value is not None:
            cond = (F.col(column) < min_value) if cond is None else (cond | (F.col(column) < min_value))
        if max_value is not None:
            cond = (F.col(column) > max_value) if cond is None else (cond | (F.col(column) > max_value))
        if cond is None:
            return CheckResult(f"range_{column}", True, {"skipped": True})
        bad_count = df.filter(cond).count()
        total = df.count()
        pct_bad = (bad_count / total) * 100 if total else 0
        passed = bad_count == 0
        return CheckResult(f"range_{column}", passed, {"column": column, "bad_count": bad_count, "total": total, "pct_bad": pct_bad})

    def run_checks(self, df: DataFrame, checks: List[Dict[str, Any]]) -> List[CheckResult]:
        results: List[CheckResult] = []
        for c in checks:
            typ = c.get("type")
            if typ == "null_pct":
                results.append(self.null_percentage_check(df, c["column"], float(c["max_percent"])))
            elif typ == "duplicate":
                results.append(self.duplicate_check(df, c.get("keys", []), int(c.get("max_duplicates", 0))))
            elif typ == "regex":
                results.append(self.regex_check(df, c["column"], c["pattern"], float(c.get("max_failed_pct", 0.0))))
            elif typ == "range":
                results.append(self.range_check(df, c["column"], c.get("min"), c.get("max")))
            else:
                # Unknown check type: open for extension (OCP)
                results.append(CheckResult("unknown_check", False, {"message": f"Unknown check type: {typ}"}))
        return results

# ---------- Data Loader / Writer (SRP) ----------

class DataIO:
    def __init__(self, spark: SparkSession, min_partitions: int = 1):
        self.spark = spark
        self.min_partitions = min_partitions

    def read(self, path: str, format: str = "parquet", **options) -> DataFrame:
        df = self.spark.read.format(format).options(**options).load(path)
        if df.rdd.getNumPartitions() < self.min_partitions:
            df = df.repartition(self.min_partitions)
        return df

    def write(self, df: DataFrame, path: str, format: str = "parquet", mode: str = "overwrite", **options) -> None:
        df.write.mode(mode).format(format).options(**options).save(path)

# ---------- Orchestrator / Application Service (High-level module) ----------

class DataQualityService:
    def __init__(self, spark: SparkSession, config: AppConfig, audit_sink: AuditSink):
        self.spark = spark
        self.config = config
        self.audit_sink = audit_sink
        self.schema_validator = SchemaValidator(spark)
        self.dq_checker = DataQualityChecker(spark)
        self.io = DataIO(spark, min_partitions=config.min_partitions)

    def run(self) -> Dict[str, Any]:
        start_ts = datetime.utcnow().isoformat()
        record = {
            "app": self.config.app_name,
            "env": self.config.env,
            "start_ts": start_ts,
            "input": self.config.input_path,
        }
        try:
            df = self.io.read(self.config.input_path)
            record["input_count"] = df.count()

            # Schema validation
            schema_ok = True
            schema_issues: List[str] = []
            if self.config.schema_path:
                self.schema_validator.load_schema_from_json(self.config.schema_path)
                schema_ok, schema_issues = self.schema_validator.validate(df)
            record["schema_ok"] = schema_ok
            record["schema_issues"] = schema_issues

            # Unique key check
            if self.config.unique_keys:
                unique_result = self.dq_checker.duplicate_check(df, self.config.unique_keys)
            else:
                unique_result = CheckResult("duplicate_check", True, {"skipped": True})

            # Run configured checks
            results = self.dq_checker.run_checks(df, self.config.checks)
            all_passed = schema_ok and unique_result.passed and all(r.passed for r in results)

            # Write output if passed
            if all_passed:
                self.io.write(df, self.config.output_path)

            # Build audit record
            record.update({
                "unique_check": {"passed": unique_result.passed, "details": unique_result.details},
                "checks": [{"name": r.name, "passed": r.passed, "details": r.details} for r in results],
                "all_passed": all_passed,
                "end_ts": datetime.utcnow().isoformat(),
            })
            self.audit_sink.write(record)
            return record
        except Exception as ex:
            record.update({"error": str(ex), "end_ts": datetime.utcnow().isoformat()})
            self.audit_sink.write(record)
            logging.exception("DataQualityService failed")
            raise

# ---------- CLI Entrypoint ----------

def init_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def main(config_path: str) -> None:
    cfg = AppConfig.from_yaml(config_path)
    spark = init_spark(cfg.app_name)
    audit_sink = FileAuditSink(cfg.audit_path)
    service = DataQualityService(spark, cfg, audit_sink)
    result = service.run()
    print(json.dumps(result, indent=2))


# ---------- Sample config (YAML) ----------
SAMPLE_CONFIG = """
app_name: DQApp
env: dev
input_path: /data/input/events
output_path: /data/output/cleaned
audit_path: ./audit
schema_path: ./schema.json
unique_keys:
  - id
checks:
  - type: null_pct
    column: user_id
    max_percent: 5.0
  - type: duplicate
    keys: [id]
    max_duplicates: 0
  - type: regex
    column: email
    pattern: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
    max_failed_pct: 1.0
  - type: range
    column: amount
    min: 0
    max: 100000
"""

# ---------- If run as script ----------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pyspark_data_quality_utility.py <config.yaml>")
        print("Sample config:\n", SAMPLE_CONFIG)
        sys.exit(1)
    main(sys.argv[1])
