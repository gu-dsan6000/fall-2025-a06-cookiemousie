"""
Problem #1 (Local to start) Solution for Homework #6
Author: Jacob Meyer
"""

#import relevant libraries 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, rand

#input paths for our s3 data (or locally first to test)

#run type...
run_type = "cluster"

#local paths for running on ec2 instance, not cluster
input_path_local = "data/sample"
output_path_local = "data/output"

#S3 paths
S3_bucket= "jm3756-assignment-spark-cluster-logs"
S3_input_path = f"s3a://{S3_bucket}/data/"
S3_output_path = f"s3a://{S3_bucket}/output/problem1/"


if run_type == "local":
    input_path = input_path_local
    output_path = output_path_local
    summary_file = os.path.join(output_path,"problem1_summary.txt")
else:
    input_path = S3_input_path
    output_path = S3_output_path
    summary_file = "/problem1_summary.txt"


LOG_LEVEL_REGEX = r"\b(INFO|WARN|ERROR|DEBUG)\b"

#build spark session
spark = SparkSession.builder \
    .appName("Problem1_Logs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#read our logs in recursivelly 
logs_df = spark.read.option("recursiveFileLookup", "true").text(input_path) \
    .withColumnRenamed("value", "log_entry")

total_lines = logs_df.count()

logs_with_levels = (
    logs_df
    .withColumn("log_level", regexp_extract(col("log_entry"), LOG_LEVEL_REGEX, 1))
    .filter(col("log_level") != "")
)

#for our summary stats
lines_with_levels = logs_with_levels.count()

#calculate our counts 
counts_df = (logs_with_levels
    .groupBy("log_level")
    .agg(count("*").alias("count"))
    .orderBy("log_level")
)

#output counts to csv
counts_output_path = os.path.join(output_path, "problem1_counts")
counts_df.write.mode("overwrite").option("header", True).csv(counts_output_path)

#sample 10 randomly from our log files and then output
sample_df = logs_with_levels.orderBy(rand()).limit(10)
sample_output_path = os.path.join(output_path, "problem1_sample")
sample_df.write.mode("overwrite").option("header", True).csv(sample_output_path)



#build our summary stats workframe...


counts_pd = counts_df.toPandas()
summary_lines = [
    f"Total log lines processed: {total_lines:,}",
    f"Total lines with log levels: {lines_with_levels:,}",
    f"Unique log levels found: {len(counts_pd)}",
    "",
    "Log level distribution:"
]

#calculate % of each log type (warn, info, error, debug)
for _, row in counts_pd.iterrows():
    pct = (row["count"] / lines_with_levels) * 100
    summary_lines.append(
        f"  {row['log_level']:<5}: {row['count']:>10,} ({pct:6.2f}%)"
    )

#output our summary file depending on local vs. cluster run
summary_path = os.path.join(output_path, "problem1_summary.txt")

if run_type == "local":
    with open(summary_path, "w") as f:
        f.write("\n".join(summary_lines))
else:
    # On cluster, save summary as single-row CSV to S3
    spark.createDataFrame([("\n".join(summary_lines),)], ["summary"]) \
        .write.mode("overwrite").option("header", True) \
        .csv(os.path.join(output_path, "problem1_summary"))

spark.stop()
print("Problem 1 complete.")