import os
from olistlib.db.utils import connect_db, import_query, execute_many_sql
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--date_init", "-i", help="Data referencia para inicio da ABT")
parser.add_argument("--date_end", "-e", help="Data referencia para fim da ABT")
parser.add_argument("--file_type", help="De onde deseja importar os arquivos?", 
                    choices=['csv', 'sqlite'])
args = parser.parse_args()

TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(TRAIN_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')

OUT_BASE_DIR = os.path.dirname(BASE_DIR)
OUT_DATA_DIR = os.path.join(OUT_BASE_DIR, "upload_olist", "data")
DB_PATH = os.path.join(OUT_DATA_DIR, "olist.db")

# # Conexão com o banco de dados
# print("\n Abrindo conexão com o banco de dados ...")
# conn = connect_db('sqlite', path=DB_PATH)
# print("OK! Conexão com o banco de dados aberta.\n")

if args.file_type == 'csv':
    table_name = 'tb_abt_{date_init}_{date_end}.csv'\
        .format(date_init=args.date_init.replace('-',''),
                date_end=args.date_end.replace('-',''))
    df = pd.read_csv(os.path.join(DATA_DIR, table_name))

if args.file_type == 'sqlite':
    conn = connect_db('sqlite', path=DB_PATH)
    table_name = 'tb_abt_{date_init}_{date_end}'\
        .format(date_init=args.date_init.replace('-',''),
                date_end=args.date_end.replace('-',''))
    df = pd.read_sql_table(table_name, conn)

print(df.head())