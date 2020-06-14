import os
from olistlib.db.utils import connect_db, import_query, execute_many_sql
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--date_init", "-i", help="Data referencia para inicio da ABT")
parser.add_argument("--date_end", "-e", help="Data referencia para fim da ABT")
args = parser.parse_args()

TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(TRAIN_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)

OUT_BASE_DIR = os.path.dirname(BASE_DIR)

DATA_DIR = os.path.join(OUT_BASE_DIR, "upload_olist", "data")
DB_PATH = os.path.join(DATA_DIR, "olist.db")

# Cria lista de dados
date = datetime.datetime.strptime(args.date_init, "%Y-%m-%d")
date_end = datetime.datetime.strptime(args.date_end, "%Y-%m-%d")
dates = []
while date <= date_end:
    dates.append(date.strftime("%Y-%m-%d"))
    date += relativedelta(months=1)

conn = connect_db('sqlite', path=DB_PATH)

# Query de features base
query_etl_base = import_query(os.path.join(TRAIN_DIR, "etl.sql"))

# Query para abt_base
query_abt_base = import_query(os.path.join(TRAIN_DIR, "make_abt.sql"))

dfs = []
for d in dates:
    query_etl = query_etl_base.format(date=d, stage="train")
    query_abt = query_abt_base.format(date=d)
    execute_many_sql(query_etl, conn)
    dfs.append(pd.read_sql_query(query_abt, conn))

df = pd.concat(dfs, axis=0, ignore_index=True)

print(df)
print(df.shape)