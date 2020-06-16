import os
from olistlib.db.utils import connect_db, import_query, execute_many_sql
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sklearn import tree
import shutil

parser = argparse.ArgumentParser()
parser.add_argument('--date', '-d', help='Data referencia para inicio da ABT')
parser.add_argument(
    '--export', help='Tipo de exportação', choices=['csv', 'sqlite'])
args = parser.parse_args()

PREDICT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(PREDICT_DIR)

TRAIN_DIR = os.path.join(SRC_DIR, 'train')

BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')
MODELS_DIR = os.path.join(BASE_DIR, 'models')

OUT_BASE_DIR = os.path.dirname(BASE_DIR)
OUT_DATA_DIR = os.path.join(OUT_BASE_DIR, "upload_olist", "data")
DB_PATH = os.path.join(OUT_DATA_DIR, "olist.db")

# Copia o arquivo etl.sql da pasta train para a pasta predict
shutil.copyfile(
    os.path.join(TRAIN_DIR, 'etl.sql'), 
    os.path.join(PREDICT_DIR, 'etl.sql'))

# Importando modelo
print("\nImportando modelo ...")
model = pd.read_pickle(os.path.join(MODELS_DIR, 'model.pkl'))
print("OK!")

query = import_query(os.path.join(PREDICT_DIR, 'etl.sql'))

# Abrindo conexão
print("\nAbrindo conexão ...")
conn = connect_db('sqlite', path=DB_PATH)
print("OK!")

# Fazendo a ETL 
print("\nFazendo a ETL ...")
query = query.format(date=args.date, stage='predict')
execute_many_sql(query, conn)
df = pd.read_sql_table('pre_abt_predict_churn', conn)
print("OK!")

# Realizando predições
print("\nRealizando predições ...")
df['churn_prob'] = model['model'].predict_proba(df[model['features']])[:, 1]
print(df[['seller_id', 'churn_prob']])
print("OK!")

# Salvando base escora
print("Salvando base escora ...")
table = df[['seller_id', 'churn_prob']]
if args.export == 'sqlite':
    table.to_sql('tb_churn_score', conn, index=False)
elif args.export == 'csv':
    table.to_csv(os.path.join(DATA_DIR, 'tb_churn_score.csv'), index=False)
print("OK!")
