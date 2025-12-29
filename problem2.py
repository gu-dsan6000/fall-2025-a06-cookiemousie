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
import numpy as np
from scipy.stats import gaussian_kde

#input paths for our s3 data (or locally first to test)

#run type...
run_type = "cluster"

#local paths for running on ec2 instance, not cluster
input_path_local = "data/sample"


#S3 paths
S3_bucket= "jm3756-assignment-spark-cluster-logs"
S3_input_path = f"s3a://{S3_bucket}/data/"


#output path fixed locally
output_path = "data/output"
os.makedirs(output_path, exist_ok=True)

input_path = input_path_local if run_type == "local" else S3_input_path

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
        spark_max("end_time").alias("last_app_end")
    ).orderBy("cluster_id")

#output our cluster summary info..
cluster_summary_csv = os.path.join(output_path, "problem2_cluster_summary.csv")
cluster_summary_df.write.mode("overwrite").option("header", True).csv(cluster_summary_csv)

#convert to pandas for easy of plotting and other stats
app_times_pd = app_times_df.toPandas()
cluster_summary_pd = cluster_summary_df.toPandas()

#Calculating our summary stats finally...
total_clusters = cluster_summary_pd.shape[0]
total_apps = app_times_pd.shape[0]

summary_file = os.path.join(output_path, "problem2_stats.txt")
with open(summary_file, "w") as f:
    f.write(f"Total unique clusters: {total_clusters}\n")
    f.write(f"Total applications: {total_apps}\n")
    f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n\n")
    f.write("Most heavily used clusters:\n")
    for _, row in top_clusters.iterrows():
        f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

#Now to plotting...

#make a bar chart here

#set some colors..
cmap = plt.get_cmap("tab10")  
colors = [cmap(i % cmap.N) for i in range(len(cluster_summary_df))]

# Create bar chart
plt.figure(figsize=(10, 6))
bars = plt.bar(
    cluster_summary_df["cluster_id"],
    cluster_summary_df["num_applications"],
    color = colors
)

# Add value labels on top of each bar
for bar in bars:
    height = bar.get_height()
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        height,
        f"{int(height)}",
        ha="center",
        va="bottom"
    )

#set label info
plt.xlabel("Cluster ID")
plt.ylabel("Number of Applications")
plt.title("Number of Applications per Cluster")

plt.tight_layout()

#output our graph...
plt.savefig("problem2_bar_chart.png")
plt.close()


#Now let's make density plot of largest cluster

#pull out data just for largest cluster
largest_cluster_id = (app_times_df.groupby("cluster_id").size().idxmax())
cluster_df = app_times_df[app_times_df["cluster_id"] == largest_cluster_id]

#make df for durations and dropping length
durations = cluster_df["duration_sec"].dropna()
durations = durations[durations > 0]
n = len(durations)

plt.figure(figsize=(10, 6))
plt.hist(
    durations,
    bins=30,
    density=True,
    alpha=0.6
)

#kde portion...
kde = gaussian_kde(durations)
x_vals = np.logspace(
    np.log10(durations.min()),
    np.log10(durations.max()),
    500
)
plt.plot(x_vals, kde(x_vals))

#Set our log scale on x-axis
plt.xscale("log")

#set labels for axis 
plt.xlabel("Job Duration (seconds, log scale)")
plt.ylabel("Density")
plt.title(
    f"Job Duration Distribution for Largest Cluster "
    f"(Cluster {largest_cluster_id}, n={n})"
)

plt.tight_layout()
plt.savefig("problem2_density_plot.png")
plt.close()

spark.stop()
print("Problem 2 complete. All 5 outputs saved.")