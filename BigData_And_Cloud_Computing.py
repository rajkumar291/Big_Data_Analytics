from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    year,
    month,
    sum as spark_sum,
    avg
)

import pandas as pd
import matplotlib.pyplot as plt

# STEP 1: Create Spark Session
spark = SparkSession.builder \
    .appName("MES_Smart_Meter_Analytics") \
    .getOrCreate()

# STEP 2: Load Dataset
file_path = "/content/Smart meter data.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

print("Dataset Schema:")
df.printSchema()

print("Sample Records:")
df.show(5)

# STEP 3: Data Cleaning
from pyspark.sql.functions import trim, col, try_to_timestamp, lit

# Remove duplicates
df = df.dropDuplicates()

# Remove rows with missing values
df = df.dropna()

# Check if 'x_Timestamp' column exists before processing it
if "x_Timestamp" in df.columns:
    # Trim x_Timestamp column to remove any leading/trailing spaces
    df = df.withColumn("x_Timestamp", trim(col("x_Timestamp")))

    # Convert Timestamp column using try_to_timestamp for robustness
    df = df.withColumn(
        "Timestamp",
        try_to_timestamp(col("x_Timestamp"), lit("dd-MM-yyyy HH:mm"))
    )

    # Drop the original x_Timestamp column
    df = df.drop("x_Timestamp")

# Remove invalid energy values
df = df.filter(col("t_kWh") >= 0)

# Voltage validation (example range)
df = df.filter(
    (col("z_Avg Voltage (Volt)") >= 180) &
    (col("z_Avg Voltage (Volt)") <= 260)
)

# Frequency validation
df = df.filter(
    (col("y_Freq (Hz)") >= 49) &
    (col("y_Freq (Hz)") <= 51)
)

print("Cleaned Data")
df.show(5)

# STEP 4: Create Date Attributes
df = df.withColumn(
    "Date",
    to_date(col("Timestamp"))
)

df = df.withColumn(
    "Year",
    year(col("Timestamp"))
)

df = df.withColumn(
    "Month",
    month(col("Timestamp"))
)

# STEP 5: Daily Consumption Aggregation
daily_consumption = df.groupBy("Date") \
    .agg(
        spark_sum("t_kWh").alias("Daily_kWh")
    ) \
    .orderBy("Date")

print("Daily Consumption:")
daily_consumption.show()

# STEP 6: Monthly Consumption Aggregation

monthly_consumption = df.groupBy(
    "Year",
    "Month"
).agg(
    spark_sum("t_kWh").alias("Monthly_kWh")
).orderBy(
    "Year",
    "Month"
)

print("Monthly Consumption:")
monthly_consumption.show()

from pyspark.sql.functions import max, desc, hour, col

# Add 'Hour' column
df_with_hour = df.withColumn("Hour", hour(col("Timestamp")))

peak = df_with_hour.groupBy("Hour") \
    .agg(
        max("t_kWh").alias("Peak_Energy_kWh")
    ) \
    .orderBy(col("Peak_Energy_kWh").desc())

peak.show()

# STEP 7: Voltage Analysis
voltage_summary = df.groupBy("Date").agg(
    avg("z_Avg Voltage (Volt)").alias("Average_Voltage")
)

print("Voltage Summary:")
voltage_summary.show()

# STEP 8: Peak Demand Analysis
peak_demand = daily_consumption.orderBy(
    col("Daily_kWh").desc()
)

print("Top Peak Demand Days:")
peak_demand.show(10)

# STEP 9: Convert to Pandas for Visualization
daily_pd = daily_consumption.toPandas()
monthly_pd = monthly_consumption.toPandas()

# STEP 10: Daily Consumption Chart
plt.figure(figsize=(12,6))
plt.plot(
    daily_pd["Date"],
    daily_pd["Daily_kWh"]
)

plt.title("Daily Energy Consumption")
plt.xlabel("Date")
plt.ylabel("Energy Consumption (kWh)")
plt.grid(True)
plt.tight_layout()
plt.savefig("daily_consumption_chart.png")
plt.show()

# STEP 11: Monthly Consumption Chart
monthly_pd["Period"] = (
    monthly_pd["Year"].astype(str)
    + "-"
    + monthly_pd["Month"].astype(str)
)

plt.figure(figsize=(10,6))
plt.plot(
    monthly_pd["Period"],
    monthly_pd["Monthly_kWh"]
)

plt.title("Monthly Energy Consumption")
plt.xlabel("Month")
plt.ylabel("Energy Consumption (kWh)")
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.savefig("monthly_consumption_chart.png")
plt.show()

#STEP 12: Calculate hourly average consumption
hourly_average_consumption = df_with_hour.groupBy("Hour") \
    .agg(
        avg("t_kWh").alias("Average_Consumption")
    ) \
    .orderBy("Hour")

hourly_pd = hourly_average_consumption.toPandas()

plt.figure(figsize=(10,5))

plt.bar(
    hourly_pd["Hour"],
    hourly_pd["Average_Consumption"]
)

plt.title("Hourly Average Energy Consumption")
plt.xlabel("Hour")
plt.ylabel("Average kWh")
plt.savefig("hourly_consumption_chart.png")
plt.show()

#STEP 13: Calculate Peak Energy by Hour
peak_pd = peak.toPandas()

plt.figure(figsize=(10,5))

plt.bar(
    peak_pd["Hour"],
    peak_pd["Peak_Energy_kWh"]
)

plt.title("Peak Energy by Hour")
plt.xlabel("Hour")
plt.ylabel("Energy (kWh)")
plt.savefig("Peak_Energy_by_Hour.png")
plt.show()

#STEP 14: Average Voltage Trend
voltage_pd = voltage_summary.toPandas()

plt.figure(figsize=(12,5))

plt.plot(
    voltage_pd["Date"],
    voltage_pd["Average_Voltage"]
)

plt.title("Average Voltage Trend")
plt.xlabel("Date")
plt.ylabel("Voltage")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("Average_Voltage_Trend.png")
plt.show()

# STEP 15: Export Results
daily_consumption.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("output/Daily_consumption")

monthly_consumption.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("output/Monthly_consumption")

print("Analysis Completed Successfully")

# Stop Spark Session
spark.stop()























