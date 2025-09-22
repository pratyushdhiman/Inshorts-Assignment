# Local base folder where data will be stored
LOCAL_DATA_FOLDER = ""

# GCS buckets for each dataset
DATASETS = {
    "User": "gs://nis_interview_task_de/user/part*",
    "Event": "gs://nis_interview_task_de/event/part*",
    "Content": "gs://nis_interview_task_de/content/part*"
}

# Retry configuration
MAX_RETRIES = 3
WAIT_SECONDS = 5
