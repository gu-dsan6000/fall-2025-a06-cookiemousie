"""
Problem #2 (Local to start) Solution for Homework #6
Author: Jacob Meyer
"""

#import relevant libraries 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, min as spark_min, max as spark_max, count as spark_count
from pyspark.sql.functions import unix_timestamp
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

#input paths for our s3 data (or locally first to test)

#run type...
run_type = "cluster"

#local paths for running on ec2 instance, not cluster
input_path_local = "data/sample"
output_path_local = "data/output"

#S3 paths
S3_bucket= "jm3756-assignment-spark-cluster-logs"
S3_input_path = f"s3a://{S3_bucket}/data/"
S3_output_path = "data/output"


if run_type == "local":
    input_path = input_path_local
    output_path = output_path_local
else:
    input_path = S3_input_path
    output_path = S3_output_path

#build spark session
spark = SparkSession.builder \
    .appName("Problem2_Logs") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#read our logs in recursivelly 
logs_df = spark.read.option("recursiveFileLookup", "true").text(input_path) \
    .withColumnRenamed("value", "log_entry") \
    .withColumn("filepath", F.input_file_name())


#extract cluster_id and app_id from folder structure using regex patterns
app_pattern = r"application_(\d+)_(\d+)/"
logs_df = logs_df.withColumn("cluster_id", regexp_extract(col("filepath"), app_pattern, 1)) \
                 .withColumn("app_id", regexp_extract(col("filepath"), app_pattern, 2))

#Get our time stamp and such
timestamp_pattern = r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
logs_df = logs_df.withColumn("timestamp", regexp_extract(col("log_entry"), timestamp_pattern, 1))
logs_df = logs_df.filter(col("timestamp") != "")

#Convert timestamp to Spark type
logs_df = logs_df.withColumn("timestamp", unix_timestamp("timestamp", "yy/MM/dd HH:mm:ss").cast("timestamp"))

#Make application level summary info
app_times_df = logs_df.groupBy("cluster_id", "app_id") \
    .agg(
        spark_min("timestamp").alias("start_time"),
        spark_max("timestamp").alias("end_time"),
        spark_count("*").alias("log_lines")
    )

app_times_df = app_times_df.withColumn("duration_sec", 
                                       (unix_timestamp("end_time") - unix_timestamp("start_time")))

#Save our timeline to CSV
timeline_csv = os.path.join(output_path, "problem2_timeline.csv")
app_times_df.orderBy("cluster_id", "start_time") \
            .write.mode("overwrite").option("header", True).csv(timeline_csv)

#Creating cluster level summary info
cluster_summary_df = app_times_df.groupBy("cluster_id") \
    .agg(
        spark_count("app_id").alias("num_applications"),
        spark_min("start_time").alias("first_app_start"),
        spark_max("end_time").alias("last_app_end"),
        spark_min("duration_sec").alias("min_duration_sec"),
        spark_max("duration_sec").alias("max_duration_sec"),
    ).orderBy("cluster_id")

cluster_summary_csv = os.path.join(output_path, "problem2_cluster_summary.csv")
cluster_summary_df.write.mode("overwrite").option("header", True).csv(cluster_summary_csv)

#convert to pandas for easy of plotting and other stats
app_times_pd = app_times_df.toPandas()
cluster_summary_pd = cluster_summary_df.toPandas()

#Calculating our summary stats finally...
total_clusters = cluster_summary_pd.shape[0]
total_apps = app_times_pd.shape[0]
avg_duration = app_times_pd['duration_sec'].mean()
min_duration = app_times_pd['duration_sec'].min()
max_duration = app_times_pd['duration_sec'].max()

summary_file = os.path.join(output_path, "problem2_stats.txt")
with open(summary_file, "w") as f:
    f.write(f"Total clusters: {total_clusters}\n")
    f.write(f"Total applications: {total_apps}\n")
    f.write(f"Average application duration (sec): {avg_duration:.2f}\n")
    f.write(f"Min application duration (sec): {min_duration}\n")
    f.write(f"Max application duration (sec): {max_duration}\n")

#Now to plotting...

#make a bar chart here
plt.figure(figsize=(10,6))
sns.barplot(x="cluster_id", y="num_applications", data=cluster_summary_pd)
plt.xticks(rotation=45)
plt.xlabel("Cluster ID")
plt.ylabel("Number of Applications")
plt.title("Number of Applications per Cluster")
plt.tight_layout()
plt.savefig(os.path.join(output_path, "problem2_bar_chart.png"))
plt.close()

#Make density plot to assess distribution of times by cluster
plt.figure(figsize=(10,6))
sns.displot(data=app_times_pd, x="duration_sec", hue="cluster_id", kind="kde", fill=True)
plt.xlabel("Application Duration (sec)")
plt.title("Application Duration Density per Cluster")
plt.savefig(os.path.join(output_path, "problem2_density_plot.png"))
plt.close()

spark.stop()
print("Problem 2 complete. All 5 outputs saved.")