import sys
import pandas as pd 

print("arguments", sys.argv)

month = sys.argv[1]


df = pd.DataFrame({"number_day": [1, 2], "number_passanger": [3, 4]})
df['month'] = month
print(df.head())

df.to_parquet(f"output_{month}.parquet")

print(f'hello pipline, month={month}')