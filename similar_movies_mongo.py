"""
Save all possible movie combination's to mongo db database
"""

import pandas as pd
import multiprocessing as mp
from pymongo import MongoClient

conn = MongoClient("mongodb://localhost:27017")

mydb = conn["movies"]

mycol = mydb["movies-similar"]

df = pd.read_csv(
    "data/u.data", sep="\t", names=["UserID", "MovieID", "Rating", "TimeStamp"]
)

movie_df = df[["UserID", "MovieID", "Rating"]].copy()

_movie_df = movie_df.groupby("UserID")


def save_df_movie(i):
    print(i)
    idx_list = _movie_df.groups[i]
    for i in range(0, len(idx_list)):
        for j in range(i + 1, len(idx_list)):
            results = {
                "Movie_1": int(movie_df.loc[idx_list[i]]["MovieID"]),
                "Movie_2": int(movie_df.loc[idx_list[j]]["MovieID"]),
                "Rating": int(
                    (movie_df.loc[idx_list[i]]["Rating"] ** 2)
                    * (movie_df.loc[idx_list[j]]["Rating"] ** 2)
                ),
            }
            mycol.insert_one(results)


if __name__ == "__main__":
    tempo_lst = list(range(1, 944))

    pool = mp.Pool(10)

    x = pool.map(save_df_movie, tempo_lst)
