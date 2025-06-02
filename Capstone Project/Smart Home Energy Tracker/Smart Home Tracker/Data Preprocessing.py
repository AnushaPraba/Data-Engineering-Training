import pandas as pd

df_logs = pd.read_csv("energy_logs.csv")
df_devices = pd.read_csv("devices.csv")
df_rooms = pd.read_csv("rooms.csv")

# Data Cleaning
df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'], errors='coerce')
df_logs['energy_used_kwh'] = pd.to_numeric(df_logs['energy_used_kwh'], errors='coerce')
df_logs.dropna(inplace=True)

# Energy usage per room
df_logs_devices = pd.merge(df_logs, df_devices[['device_id', 'room_id']], on='device_id', how='left')
df_full = pd.merge(df_logs_devices, df_rooms, on='room_id', how='left')

room_total = df_full.groupby('room_name')['energy_used_kwh'].sum()
room_avg = df_full.groupby('room_name')['energy_used_kwh'].mean()

print("Total Energy Usage per Room:",room_total)
print("Average Energy Usage per Room:",room_avg)
