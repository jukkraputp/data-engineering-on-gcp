from datetime import datetime

import pandas as pd


def convert_date_format(value):
    # example value: 14-Jan-09
    date_obj = datetime.strptime(value, "%d-%b-%y").date()
    return date_obj


df = pd.read_csv("breakfast_transactions.csv")
print(df.head())

df["new_week_end_date"] = df["WEEK_END_DATE"].map(convert_date_format)
print(df.head())