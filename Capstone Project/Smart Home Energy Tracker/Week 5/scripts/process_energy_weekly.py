import pandas as pd
from pathlib import Path

df = pd.read_csv("energy_logs.csv")

df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
df['date'] = df['timestamp'].dt.date


daily_usage = df.groupby(['device_id', 'date'])['energy_used_kwh'].sum().reset_index()

alerts = daily_usage[daily_usage['energy_used_kwh'] > 10]

Path("reports").mkdir(parents=True, exist_ok=True)

if not alerts.empty:
    alerts.to_csv("reports/alerts.csv", index=False)
    print("Alert: High energy usage found! Alerts saved in reports/alerts.csv")
else:
    print("All devices within safe energy limits.")
