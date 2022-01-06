# 
# File:   convert_db_to_csv.py
# Author: Maya Arbel-Raviv
#

import sqlite3
import pandas as pd
conn = sqlite3.connect("results.db")
query = "SELECT * FROM results" 
df = pd.read_sql(query, conn)
df.to_csv("results.csv")
