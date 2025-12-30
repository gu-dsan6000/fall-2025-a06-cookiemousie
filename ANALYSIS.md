# Assignment: Analysis of Real World Spark Cluster Data
### Author: Jacob Meyer


## Key Findings 

- Across 33.2M log lines, we found 27.4M that had log levels 
- We found three unique log level values with "INFO" accounting for 99.92% of all log levels
- One cluster accounted for the vast majority of applications at 181 out of 194 in the log data

## Performance Observations

- The execution time with the entire dataset and utilizing the spark cluster can still be substantial leading to errors being costly
- Testing locally on a sample of the data is import due to the performance and time considerations

## Problem 1 Strategy

- Set up script to read in local smaller data sample for testing due to performance concerns 
- For either option, read in the log data recursively to data frame
- Manipulate the data to create summary statistics and sample 10 rows randomly 
- Output these summary statistics and sample rows to CSV 

## Problem 2 Strategy

- Set up script to read in local smaller data sample for testing due to performance concerns similar to problem 1
- After reading in the log data similarly to problem 1, extract cluster and application IDs using regex
- Summarize data at cluster as well as application levels to compare clusters overall and then activity within our top cluster
- Create duration statistic for each cluster and application ID combination based upon start and end time of run
- Create plots (detailed below) showing number of applications by cluster and duration across our top cluster

## Visualizations

### Problem 2 Bar Chart
![Number of applications per cluster](data\output\problem2_bar_chart.png)

- The bar chart shows that the top cluster accounts for the majority of the applications in the log dataset at 181 applications out of 194 total

### Problem 2 Density of Biggest Cluster
![Density plot](data\output\problem2_density_plot.png)

- This graphs shows the job duration density for our top cluster with a log scale on the x-axis
- We can tell from this graph that most jobs are very short but some are extremely long on the right edge showing jobs that last hours compared to the typical 0 to 20 minutes of most jobs. 
- The KDE peaks around 10^3 (1000 seconds or 16.66 minutes) suggesting the typical job lasts around 17 minutes 
- Overall, this graph shows job duration for the top cluster is heavily right skewed with outliers that are extremely longer than the typical job 