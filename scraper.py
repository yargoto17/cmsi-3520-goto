import mechanicalsoup
import pandas as pd
import sqlite3

url = "https://en.wikipedia.org/wiki/List_of_best-selling_music_artists"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)

th = browser.page.find_all("th")
artist = [value.text.replace("\n", "") for value in th]
artist = artist[7:16]

td = browser.page.find_all("td")
columns = [value.text.replace("\n", "") for value in td]
columns = columns[0:54]

column_names = ["Country",
                "Period_active",
                "Release_year_of_first_charted_record",
                "Genre",
                "Total_certified_units_from_available_markets",
                "Claimed_sales"]

dictionary = {"Artist" : artist}

for idx, key in enumerate(column_names):
    dictionary[key] = columns[idx:][::6]
    
df = pd.DataFrame(data = dictionary)
print(df.head())
print(df.tail())

connection = sqlite3.connect("artists_distro.db")
cursor = connection.cursor()
cursor.execute("create table artists (Artist, " + ",".join(column_names) + ")")
for i in range(len(df)):
    cursor.execute("insert into artists values (?,?,?,?,?,?,?)", df.iloc[i])

connection.commit()

connection.close()



