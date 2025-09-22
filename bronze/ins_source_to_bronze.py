import subprocess
import os
import time
import glob
from bronze import config

max_retries = config.MAX_RETRIES

local_folder = config.LOCAL_DATA_FOLDER

os.makedirs(local_folder, exist_ok=True)

data_sets=config.DATASETS

for dataset,bucket in data_sets.items():
    target_path = os.path.join(local_folder, dataset)
    os.makedirs(target_path, exist_ok=True)
    result= subprocess.run(["gsutil", "-m", "cp", "-r", bucket, target_path])

    if result.returncode==0 and not glob.glob(os.path.join(target_path, "*.gstmp")):
        print(f"{dataset} download complete.")
        continue

print(f'dataload failed, retrying up to {max_retries} times...')

for attempt in range(1, max_retries + 1):
    for tmp in glob.glob(os.path.join(target_path, "*.gstmp")):
        os.remove(tmp)

    result = subprocess.run(
        ["gsutil", "-m", "cp", "-r", bucket, target_path],
        capture_output=True, text=True
    )

    if result.returncode == 0 and not glob.glob(os.path.join(target_path, "*.gstmp")):
        print(f"{dataset} download complete on retry {attempt}.")
        break
    else:
        print(f"{dataset} retry {attempt} failed: {result.stderr.strip()}")
        if attempt < max_retries:
            wait_time = attempt * 5
            print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            print(f"{dataset} download failed after {max_retries} retries.")