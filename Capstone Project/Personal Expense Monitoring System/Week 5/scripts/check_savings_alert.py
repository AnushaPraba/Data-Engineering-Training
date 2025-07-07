# scripts/check_savings_alert.py

import pandas as pd

df = pd.read_csv("outputs/monthly_totals.csv", index_col=0)

THRESHOLD = 20000
alerts = df[df["amount"] > THRESHOLD]

if not alerts.empty:
    print("Savings Alert! These months exceeded the spending threshold:")
    print(alerts)
else:
    print("All months are within savings threshold.")
