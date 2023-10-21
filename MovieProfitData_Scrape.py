#Movie Profit Data

import pandas as pd
import sqlite3

df = pd.read_csv('movie_profit.csv')
print(df.head())
print(df.tail())


connection = sqlite3.Connection("MovieProfits.db") 
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS MovieProfits.db")
#create table

df.to_sql("MovieProfits", connection)
connection.close()