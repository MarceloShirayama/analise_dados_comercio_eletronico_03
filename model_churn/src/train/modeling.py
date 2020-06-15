import os
from olistlib.db.utils import connect_db, import_query, execute_many_sql
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sklearn import tree

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
MODELS_DIR = os.path.join(BASE_DIR, 'models')

OUT_BASE_DIR = os.path.dirname(BASE_DIR)
OUT_DATA_DIR = os.path.join(OUT_BASE_DIR, "upload_olist", "data")
DB_PATH = os.path.join(OUT_DATA_DIR, "olist.db")

# Extração da base de dados
print("\nExtraindo base de dados")
# Extração de base dados csv
if args.file_type == 'csv':
    table_name = 'tb_abt_{date_init}_{date_end}.csv'\
        .format(date_init=args.date_init.replace('-',''),
                date_end=args.date_end.replace('-',''))
    df = pd.read_csv(os.path.join(DATA_DIR, table_name))

# Extração de base dados do Banco de Dados
elif args.file_type == 'sqlite':
    conn = connect_db('sqlite', path=DB_PATH)
    table_name = 'tb_abt_{date_init}_{date_end}'\
        .format(date_init=args.date_init.replace('-',''),
                date_end=args.date_end.replace('-',''))
    df = pd.read_sql_table(table_name, conn)
print("OK! Término da extração")

# Ajuste do modelo de machine learning
print("\nAjuste do modelo ...")
features = df.columns[3:-2]
target = 'flag_churn'

X = df[features]
y = df[target]

clf = tree.DecisionTreeClassifier(max_depth=8)
clf.fit(X, y)
print("OK!")

# Váriaveis mais importantes para o modelo tomar decisão
print("\nVáriaveis mais importantes para o modelo tomar decisão")
feature_importance = pd.Series(clf.feature_importances_, \
    index=features).sort_values()
print(feature_importance)

# Salvando o modelo
print("\nSalvando o modelo ...")
model = pd.Series([features, clf], index=['features', 'model'])
model.to_pickle(os.path.join(MODELS_DIR, 'model.pkl'))
print("OK!")
