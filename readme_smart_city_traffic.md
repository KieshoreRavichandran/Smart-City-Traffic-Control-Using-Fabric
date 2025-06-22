# Smart City Traffic Control - Data Platform

## 1. Introduction
This project implements a comprehensive Smart City Traffic Control platform using Microsoft Fabric and related data services. The primary goal is to collect, process, and analyze real-time and batch traffic-related data to provide actionable insights for city traffic management. The solution ingests data from sensors, weather systems, roadwork schedules, and traffic incidents, and visualizes it via Power BI for stakeholders.

### Role:
Data Engineer – responsible for data ingestion, transformation logic, Delta Lake setup, and dashboard enablement.

### Technologies Used:
- Microsoft Fabric (Data Factory, Lakehouse, Spark Notebooks)
- OneLake
- Delta Lake
- PySpark
- Power BI
- GitHub

---

## 2. Architecture
### 2.1 Architecture Diagram

![alt text](Architecture_fabric-2.png)

### 2.2 Component Overview
- **Traffic Sensors**: Provide real-time vehicle data like count, speed, and occupancy.
- **Weather API**: External data providing daily weather information.
- **Incident Reports**: Logged by city authorities, includes severity, duration, and location.
- **Roadworks**: Scheduled maintenance affecting traffic.
- **Microsoft Fabric Pipelines**: Orchestrate ingestion into OneLake staging tables.
- **Spark Notebooks**: Used for heavy transformation logic and window-based analytics.
- **Delta Lake**: Versioned, optimized storage for querying enriched traffic data.
- **Power BI**: Interactive dashboard for visualizing congestion trends and alerts.

---

## 3. Implementation

### 3.1 Data Ingestion
- **Traffic Data**: Ingested as CSV into `stg_traffic`
- **Weather Data**: JSON format ingested into `stg_weather`
- **Incidents**: CSV files ingested into `stg_incidents`
- **Roadworks**: Excel files ingested into `stg_roadworks`

All datasets are copied using Dataflows Gen2 or Copy Data activity into the Fabric Lakehouse paths under `/Files/raw/*` and converted to tables in `/Tables/stg_*`.

### 3.2 Data Transformation
- **Type Casting**: All numeric, timestamp, and string fields are cast to proper types.
- **Congestion Index**: `vehicle_count * occupancy_rate / avg_speed`
- **Joins**:
  - With weather on `date`
  - With incidents based on ±15 minutes interval window
  - With roadworks on `intersection_id` and `date` between `start_date` and `end_date`
- **Rolling Metrics**:
  - 3-hour windowed average for congestion index, speed, and volume.

### 3.3 Delta Lake Output
Enriched data is stored in Delta format at `Tables/traffic_enriched_delta`, enabling Direct Lake queries in Power BI.

---

## 4. Power BI Dashboard
- **Connected to**: `traffic_enriched_delta`
- **Key Visualizations**:
  - Heatmap of congestion hotspots
  - Trends of congestion over time
  - Peak hour severity vs normal hours
  - Incidents and Roadwork overlays
  - Predictive alerts for upcoming congestions

---

## 5. Key Features & Validation
- **Error Handling**: All joins use `coalesce()` and null-safe logic.
- **Performance**: Aggregations use partitioned windows; Delta Lake indexing optimizes read speed.
- **Validation**:
  - Row count comparison between raw and transformed tables
  - Logic unit testing inside Spark Notebook

---

## 6. Project Deliverables
- ETL Code: PySpark notebook or `ETL_Pipeline.py`
- Power BI Dashboard (linked to Direct Lakehouse)
- Architecture Diagram (attached image)
- Documentation (this README and a separate overview document)

---

## 7. Challenges & Improvements
- **Time-based joins** were difficult to optimize without timestamp granularity.
- **Recommendation**: Ingest all timestamps in UTC.
- **Next Steps**: Add ML model to forecast congestion and auto-alert city authorities.

