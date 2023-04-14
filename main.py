"""
Make movie recommendations using previous user data and ratings
"""

import pandas as pd
from pymongo import MongoClient

conn = MongoClient("mongodb://localhost:27017")

film_idx = 50 # Movie ID

mydb = conn["movies"]

mycol = mydb["movies-similar"]

_mycol = mydb["movies"]

df_1 = pd.DataFrame(list(mycol.find({"Movie_1": film_idx})))

df_2 = pd.DataFrame(list(mycol.find({"Movie_2": film_idx})))

df_2 = df_2[["_id", "Movie_2", "Movie_1", "Rating"]]

df_3 = df_2.rename(columns={"Movie_1": "Movie_2", "Movie_2": "Movie_1"})

df_combined = pd.concat([df_1, df_3], axis=0, ignore_index=True)

df_combined = df_combined[df_combined["Rating"] > 0.97* 625] # accept only those combinations having 97% approval

# Group by movie 1 and 2 and get count of occurances 
_df_combined = df_combined.groupby(["Movie_1", "Movie_2"]).count().sort_values("Rating")

print(_df_combined)

top_five = _df_combined.iloc[-5:].reset_index()

my_movie = _mycol.find_one({"ID": int(film_idx)})["Movie"]
print(f"My Movie: {my_movie}")
print("Matches:")
for i in range(0, 5):
    idx = top_five.loc[i]["Movie_2"]
    match = _mycol.find_one({"ID": int(idx)})["Movie"]
    print(match)
