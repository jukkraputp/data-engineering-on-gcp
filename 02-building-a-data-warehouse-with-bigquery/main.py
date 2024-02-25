import sys
import datetime
import json

from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd

from bigquery_job_config import JobConfig


# โค้ดส่วนนี้จะเป็นการใช้ Keyfile เพื่อสร้าง Credentials เอาไว้เชื่อมต่อกับ BigQuery
# โดยการสร้าง Keyfile สามารถดูได้จากลิ้งค์ About Google Cloud Platform (GCP)
# ที่หัวข้อ How to Create Service Account
#
# การจะใช้ Keyfile ได้ เราต้องกำหนด File Path ก่อน ซึ่งวิธีกำหนด File Path เราสามารถ
# ทำได้โดยการเซตค่า Environement Variable ที่ชื่อ KEYFILE_PATH ได้ จะทำให้เวลาที่เราปรับ
# เปลี่ยน File Path เราจะได้ไม่ต้องกลับมาแก้โค้ด
# keyfile = os.environ.get("KEYFILE_PATH")
#
# แต่เพื่อความง่ายเราสามารถกำหนด File Path ไปได้เลยตรง ๆ
keyfile = "service_account_config.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info)

# โค้ดส่วนนี้จะเป็นการสร้าง Client เชื่อมต่อไปยังโปรเจค GCP ของเรา โดยใช้ Credentials ที่
# สร้างจากโค้ดข้างต้น
project_id = service_account_info['project_id']
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

if (sys.argv[0] is not None):
    jobs = sys.argv
else:
    jobs = ['breakfast_products', 'breakfast_stores', 'breakfast_transactions']

for job in jobs:
    # โค้ดส่วนนี้เป็นการ Configure Job ที่เราจะส่งไปทำงานที่ BigQuery โดยหลัก ๆ เราก็จะกำหนดว่า
    # ไฟล์ที่เราจะโหลดขึ้นไปมีฟอร์แมตอะไร มี Schema หน้าตาประมาณไหน
    job_config = JobConfig.create_table(job)

    if job_config is not None:
        # โค้ดส่วนนี้จะเป็นการอ่านไฟล์ CSV และโหลดขึ้นไปยัง BigQuery
        file_path = f"{job}.csv"
        with open(file_path, "rb") as f:
            table_id = f"{project_id}.{job.replace('_', '.')}"
            df = pd.read_csv(f, header=0)
            df.columns = [x.lower() for x in df.columns]
            if (job == 'breakfast_transactions'):
                df['week_end_date'] = df['week_end_date'].astype(str).apply(
                    lambda x: datetime.datetime.strptime(x, '%d-%b-%y').date())
            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config)
            job.result()

        # โค้ดส่วนนี้จะเป็นการดึงข้อมูลจากตารางที่เราเพิ่งโหลดข้อมูลเข้าไป เพื่อจะตรวจสอบว่าเราโหลดข้อมูล
        # เข้าไปทั้งหมดกี่แถว มีจำนวน Column เท่าไร
        table = client.get_table(table_id)
        print(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
