from titanic_task.load_to_gcs import load_to_gcs
from titanic_task.load_from_gcs_to_bigquery import load_from_gcs_to_bigquery

class Task:
    def _load_to_gcs(): 
        return load_to_gcs()
    
    def _load_from_gcs_to_bigquery():
        return load_from_gcs_to_bigquery()
    