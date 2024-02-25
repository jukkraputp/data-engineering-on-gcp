import streamlit as st
import pandas as pd
import numpy as np
import configparser
import json
from google.oauth2 import service_account
from google.cloud import bigquery

CONFIG_FILE = 'local.conf'

parser = configparser.ConfigParser()
parser.read(CONFIG_FILE)

dataset_name = parser.get('bigquery', 'dataset_name')
model_name = parser.get('bigquery', 'model_name')

st.title("Titanic Survivor Predictor")
# Sex,Fare,Age,Pclass,
sex_option = st.selectbox(
    "Sex",
    ("male", "female"),
    index=None,
    placeholder="Select sex...",
)

fare_option = st.text_input(
    "Fare",
    placeholder="Fare of passenger",
)

age_option = st.selectbox(
    "age",
    range(1, 100),
    index=None,
    placeholder="Select age...",
)

pclass_option = st.selectbox(
    "pclass",
    range(1, 4),
    index=None,
    placeholder="Select pclass...",
)

if st.button("Predict"):
    # read model
    keyfile_bigquery_and_gcs = parser.get(
        "gcp", "bigquery_and_gcs_service_account")
    service_account_info_bigquery_and_gcs = json.load(
        open(keyfile_bigquery_and_gcs))
    credentials_bigquery_and_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery_and_gcs
    )
    project_id = "data-engineering-on-gcp-410507"
    client = bigquery.Client(credentials=credentials_bigquery_and_gcs)
    location = "us-central1"
    # make prediction
    df = pd.DataFrame()

    query = f"""
    select * from ml.predict(
        model {dataset_name}.{model_name}, (
        select '{sex_option}' as Sex,  
                {fare_option} as Fare,  
                {age_option} as Age, 
                {pclass_option} as Pclass
        )
    )
    """

    df = client.query(query).to_dataframe()
    print(df['predicted_label_probs'][0])

    show_survive_chance = True
    if show_survive_chance:
        result = f"Survive chance: {df['predicted_label_probs'][0][0]['prob'] * 100}%"
    else:
        result = 'is Survived: '
        if (df['predicted_label'][0] == 0):
            result += 'No'
        else:
            result += 'Yes'
    st.subheader(result)

