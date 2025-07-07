# scripts/expense_processing.py

import pandas as pd
import numpy as np
import os

# Load data
df = pd.read_csv("data/expenses_cleaned.csv")

# Clean
df['amount'] = df['amount'].replace('[₹,]', '', regex=True).astype(float)
df['expense_date'] = pd.to_datetime(df['expense_date'])
df['month'] = df['expense_date'].dt.to_period('M')

# Monthly totals & averages
monthly_totals = df.groupby('month')['amount'].sum()
monthly_avg = df.groupby('month')['amount'].mean()

# Category breakdown
category_breakdown = df.groupby(['month', 'category'])['amount'].sum().unstack().fillna(0)

# Save results
os.makedirs("outputs", exist_ok=True)
monthly_totals.to_csv("outputs/monthly_totals.csv")
monthly_avg.to_csv("outputs/monthly_avg.csv")
category_breakdown.to_csv("outputs/category_breakdown.csv")

print("Expense summary files saved to /outputs/")
