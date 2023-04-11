"""
Save Movie to Mongodb database
"""

import pandas as pd
import multiprocessing as mp
from pymongo import MongoClient

conn = MongoClient("mongodb://localhost:27017")

mydb = conn["movies"]

mycol = mydb["movies"]

df = pd.read_csv("data/u.item", sep="|", encoding="ISO-8859-1", header=None)

df_ = df.iloc[:, :2]

df_.columns = ["ID", "Movie"]

mongo_db = lambda row: mycol.insert_one({"ID": row.ID, "Movie": row.Movie})

df_["new"] = df_.apply(mongo_db, axis=1)
