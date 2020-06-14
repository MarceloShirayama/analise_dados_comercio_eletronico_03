import pandas as pd
from olistlib.db.utils import import_query, connect_db
from sklearn import tree, metrics

# Importando a query
query_path = '/home/marcelo/desenvolvimento/proj_ex/teo_calvo/olist_do_teo/prepara_abt/query02.sql'
query = import_query(query_path)

datas = ['2017-01-01',
        '2017-02-01',
        '2017-03-01',
        '2017-04-01',
        '2017-05-01',
        '2017-06-01',
        '2017-07-01',
        '2017-08-01',
        '2017-09-01'
        ]

conn = connect_db('sqlite', 
    path='/home/marcelo/desenvolvimento/proj_ex/teo_calvo/olist_do_teo/upload_olist/data/olist.db'
    )

dfs = []
for data in datas:
    query_formatada = query.format(date = data)
    df_tmp = pd.read_sql_query(query_formatada, conn)
    dfs.append(df_tmp)

abt = pd.concat(dfs, axis=0, ignore_index=True)

abt['dt_referencia'].unique()

print("média de churn na base: ", abt['flag_churn'].mean())

# definição da váriavel target
target = 'flag_churn'
# definição das features para fazer a previsão
features = abt.columns[3:-2]

#Definição do classificador e seu ajuste
clf = tree.DecisionTreeClassifier(max_depth=10)
clf.fit(abt[features], abt[target])

# Criação da base preditiva
y_pred = clf.predict(abt[features])
y_prob = clf.predict_proba(abt[features])

# Acurácia
acc = metrics.accuracy_score(abt[target], y_pred)
print(f"acuracia: {acc}")

# ROC
auc = metrics.roc_auc_score(abt[target], y_prob[:,1])
print(f"roc: {auc}")

# Váriavel mais importante para o modelo tomar decisão
features_importance = pd.Series(clf.feature_importances_, index=features)
features_importance.sort_values(inplace=True)
print(features_importance)
