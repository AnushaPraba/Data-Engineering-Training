import pandas as pd
import numpy as np

expenses_df = pd.read_csv('expenses.csv')
print(expenses_df)
# Cleaning amount column to remove symbols
expenses_df['amount'] = expenses_df['amount'].replace('[₹,]', '', regex=True).astype(float)

# Converting to datetime object
expenses_df['expense_date'] = pd.to_datetime(expenses_df['expense_date'])

# Extracting month values
expenses_df['month'] = expenses_df['expense_date'].dt.to_period('M')

# Monthly Total
monthly_total = expenses_df.groupby('month')['amount'].sum()
print("Monthly Total:",monthly_total)

# Monthly Average
monthly_avg = expenses_df.groupby('month')['amount'].mean()
print("Monthly Average:",monthly_avg)

# Category-wise Monthly Total
categories_df = pd.read_csv('categories.csv')
# Merge on category_id
df = expenses_df.merge(categories_df, on='category_id')
# Grouping by category_name
monthly_expense = df.groupby(['month', 'category_name'])['amount'].sum().unstack().fillna(0)
print(monthly_expense)
