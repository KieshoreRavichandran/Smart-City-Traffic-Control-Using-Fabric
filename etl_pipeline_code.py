from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName("SmartCityTrafficETL").getOrCreate()

# --- Step 0: Read base tables ---
traffic = spark.read.table("stg_traffic")
weather = spark.read.table("stg_weather")
incidents = spark.read.table("stg_incidents")
roadworks = spark.read.table("stg_roadworks")

# --- Step 1: Data Type Casting & Normalization ---
traffic = (
    traffic
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("intersection_id", F.col("intersection_id").cast(IntegerType()))
    .withColumn("vehicle_count", F.col("vehicle_count").cast(IntegerType()))
    .withColumn("avg_speed", F.col("average_speed").cast(DoubleType()))
    .withColumn("occupancy_rate", F.col("occupancy_rate").cast(DoubleType()))
    .drop("average_speed")
)

weather = weather.withColumn("date", F.to_date("date"))

incidents = (
    incidents
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumnRenamed("location", "intersection_id")
    .withColumn("intersection_id", F.col("intersection_id").cast(IntegerType()))
)

roadworks = (
    roadworks
    .withColumnRenamed("Prop_0", "intersection_id")
    .withColumnRenamed("Prop_1", "start_date")
    .withColumnRenamed("Prop_2", "end_date")
    .withColumnRenamed("Prop_3", "expected_delay_minutes")
    .withColumnRenamed("Prop_4", "description")
    .withColumn("intersection_id", F.col("intersection_id").cast(IntegerType()))
    .withColumn("start_date", F.to_date("start_date"))
    .withColumn("end_date", F.to_date("end_date"))
    .withColumn("expected_delay_minutes", F.col("expected_delay_minutes").cast(IntegerType()))
)

# --- Step 2: Derive Congestion Index ---
traffic = traffic.withColumn(
    "congestion_index",
    F.when(
        F.col("avg_speed") > 0,
        F.col("vehicle_count") * F.col("occupancy_rate") / F.col("avg_speed")
    ).otherwise(F.lit(0))
)

# --- Step 3: Time-based Features ---
traffic = (
    traffic
    .withColumn("hour_of_day", F.hour("timestamp"))
    .withColumn("date", F.to_date("timestamp"))
    .withColumn("is_peak_hour", F.when((F.col("hour_of_day").between(7,9)) | (F.col("hour_of_day").between(16,19)), 1).otherwise(0))
)

# --- Step 4: Join with Weather ---
traffic_weather = traffic.join(weather, on="date", how="left")
traffic_weather = traffic_weather.withColumn(
    "is_severe_weather",
    F.when((F.col("weather_condition").isin("Snow", "Storm", "Fog")) | (F.col("precipitation_mm") > 5), 1).otherwise(0)
)

# --- Step 5: Join with Incidents ---
time_window_start = F.col("timestamp") - F.expr("INTERVAL 15 minutes")
time_window_end = F.col("timestamp") + F.expr("INTERVAL 15 minutes")

traffic_incidents = traffic_weather.join(
    incidents,
    (traffic_weather["intersection_id"] == incidents["intersection_id"]) &
    (incidents["timestamp"].between(time_window_start, time_window_end)),
    how="left"
).withColumn(
    "incident_impact",
    F.coalesce(F.col("severity") * F.col("duration_minutes"), F.lit(0))
)

# --- Step 6: Join with Roadworks ---
traffic_enriched = traffic_incidents.join(
    roadworks,
    (traffic_incidents["intersection_id"] == roadworks["intersection_id"]) &
    (traffic_incidents["date"].between(roadworks["start_date"], roadworks["end_date"])),
    how="left"
).withColumn(
    "roadwork_delay_minutes",
    F.coalesce(F.col("expected_delay_minutes"), F.lit(0))
)

# --- Step 7: Rolling 3-Hour Aggregates ---
traffic_enriched = traffic_enriched.withColumn("ts_long", F.col("timestamp").cast("long"))
window_3h = (
    Window.partitionBy("intersection_id")
    .orderBy("ts_long")
    .rangeBetween(-3 * 3600, 0)
)

traffic_enriched = (
    traffic_enriched
    .withColumn("avg_congestion_3h", F.avg("congestion_index").over(window_3h))
    .withColumn("avg_speed_3h", F.avg("avg_speed").over(window_3h))
    .withColumn("veh_count_3h", F.avg("vehicle_count").over(window_3h))
    .drop("ts_long")
)

# --- Step 8: Write to Delta Table ---
(
    traffic_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("traffic_enriched")
)
