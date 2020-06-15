import os
from olistlib.db.utils import connect_db, import_query, execute_many_sql
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--date_init", "-i", help="Data referencia para inicio da ABT")
parser.add_argument("--date_end", "-e", help="Data referencia para fim da ABT")
parser.add_argument("--save_db", "-db", help="Deseja salvar no banco de dados?", action='store_true')
parser.add_argument("--save_file", "-f", help="Deseja salvar em um arquivo?", action='store_true')
args = parser.parse_args()

TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(TRAIN_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')

OUT_BASE_DIR = os.path.dirname(BASE_DIR)
OUT_DATA_DIR = os.path.join(OUT_BASE_DIR, "upload_olist", "data")
DB_PATH = os.path.join(OUT_DATA_DIR, "olist.db")

# Cria lista de dados
date = datetime.datetime.strptime(args.date_init, "%Y-%m-%d")
date_end = datetime.datetime.strptime(args.date_end, "%Y-%m-%d")
dates = []
while date <= date_end:
    dates.append(date.strftime("%Y-%m-%d"))
    date += relativedelta(months=1)

# Conexão com o banco de dados
print("\n Abrindo conexão com o banco de dados ...")
conn = connect_db('sqlite', path=DB_PATH)
print("OK! Conexão com o banco de dados aberta.")

print("\n Executando a extração de dados.")
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
print("OK! Término da extração de dados.")

if args.save_db:
    print('\nSalvando dados no banco de dados ...')
    table_name = 'tb_abt_{date_init}_{date_end}'\
        .format(date_init=args.date_init.replace('-',''),
                date_end=args.date_end.replace('-',''))
    df.to_sql(table_name, conn, index=False, if_exists='replace')
    print("OK! Dados salvos.")

if args.save_file:
    print('\nSalvando dados no arquivo ...')
    table_name = 'tb_abt_{date_init}_{date_end}.csv'\
        .format(date_init=args.date_init.replace('-',''),
                date_end=args.date_end.replace('-',''))
    df.to_csv(os.path.join(DATA_DIR, table_name), index=False)
    print("OK! Dados salvos.")
