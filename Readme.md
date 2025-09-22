# Inshorts Data Engineering Assignment

## Project Overview
This project implements a **Medallion Architecture (Bronze–Silver–Gold)** pipeline on Inshorts user, event, and content data using **PySpark**.  

### Common Spark Utilities
- **`spark_utils.py`**:  
  - Contains a helper function to create a **SparkSession** with consistent configs.  
  - Example:
    ```python
    from spark_utils import get_spark_session

    spark = get_spark_session("ins_data_analysis")
    ```
  - This ensures all scripts use the same Spark setup (memory configs, app name, local mode).  

- **Bronze** → Raw ingestion of CSVs from Google Cloud Storage.  
- **Silver** → Cleaning and standardization of Users, Events, and Content datasets.  
- **Gold** → Integrated dataset and analytical queries to derive insights on user engagement, content consumption, and campaign effectiveness.  

---

## Architecture
    +---------+        +---------+        +---------+
    |  Bronze | -----> |  Silver | -----> |   Gold  |
    +---------+        +---------+        +---------+
   (Raw GCS)       (Cleaned tables)   (Integrated fact table
                                      + insights: funnel,
                                      retention, churn)

---

## Dataset Description

### Users (`user_silver`)
- `deviceid`: unique user/device id  
- `lang`: user language  
- `district`: user district  
- `platform`: OS (Android, iOS)  
- `install_dt`: install date  
- `campaign_id`: marketing campaign identifier (nullable)  

### Events (`event_silver`)
- `deviceid`: user/device id  
- `content_id`: content identifier  
- `eventtimestamp`: raw event timestamp (ms)  
- `event_ts`: converted timestamp  
- `timespent`: duration spent on content  
- `eventname`: type of event (Shown, Opened, Front, Back, Shared)  

### Content (`content_silver`)
- `_id`: content id  
- `createdAt`: article creation timestamp  
- `newsLanguage`: content language (ENGLISH/HINDI/OTHER)  
- `categories_array`: list of categories  
- `author`: content author (system ID or “Unknown”)  

---

## Pipeline Steps

### Bronze
Scripts in `bronze/` handle ingestion from GCS to local raw storage.  

- **`ins_source_to_bronze.py`**  
  - Copies raw CSV files (users, events, content) from Google Cloud Storage.  
  - Organizes them into local folders for downstream processing.  
  - Reads configuration (local folder paths, GCS bucket URIs, retry policy) from **`bronze/config.py`**.  

- **`ins_raw_temp_tables.py`**  
  - Reads the raw ingested data into temporary Spark DataFrames.  
  - Serves as the entry point for moving raw data into the Silver layer.  

**Configuration:**  
All Bronze parameters are centralized in **`bronze/config.py`**:  
- `LOCAL_DATA_FOLDER` → base local directory for storing raw files.  
- `DATASETS` → mapping of dataset names to GCS bucket paths.  
- `MAX_RETRIES`, `WAIT_SECONDS` → retry policy for failed downloads.  

**Flow diagram:**  

    +---------+        +-------------------+        +------------------+
    |config.py| -----> |  Bronze Ingestion | -----> |   Local Folders  |
    +---------+        +-------------------+        +------------------+
    (parameters)            (uses config)            (/gcs_data/User,
                                                     /gcs_data/Event,
                                                     /gcs_data/Content)


### Silver
Scripts in silver/ clean and standardize each dataset individually.

-**`ins_users_data.py`**

 - Drops null deviceid.

 - Normalizes platform (ANDROID / IOS).

 - Casts install_dt into proper Date format.

 - Deduplicates user rows.

-**`ins_events_data.py`**

 - Converts eventtimestamp (epoch millis) → event_ts (timestamp).

 - Casts timespent to double.

 - Keeps only valid event types (Shown, Opened, Front, Back, Shared).

 - Drops nulls in key columns (deviceid, content_id).

 - Deduplicates event rows.

-**`ins_content_data.py`**

 - Casts createdAt into timestamp.

 - Splits categories string → array.

 - Fills missing authors with "Unknown".

 - Deduplicates content rows.

 - Each script saves a cleaned dataset (*_silver) for analysis.

### Gold
Scripts in gold/ produce insights, split by dataset and combined analysis.

-**`ins_users_analysis.py`**

 - Total users.

 - Installs over time (daily/weekly/monthly).

 - Distribution by platform, language, district.

 - Campaign performance.

-**`ins_content_analysis.py`**

- Total content items.

- Publishing trends (daily, monthly).

- Language distribution.

- Category distribution.

- Top authors.

- ins_events_analysis.py

- Total events.

- Distribution by event type.

- DAU / WAU / MAU.

- Timespent analysis.

- Outlier detection.

- Top active users.

-**`ins_combined_analysis.py`**

- Funnel performance (Shown → Opened → Front → Back → Shared).

- Engagement by platform, campaign, and content category.

- Retention (Day-1, Week-1, Month-1).

- Churn analysis (weekly churn + category-switching churn).
---

## How to Run

### 1. Setup environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
