# -*- coding: utf-8 -*-
"""
Spyder Editor

@author Jonathan Bispo
"""

import pandas as pd

import json
with open("config.json",encoding="utf-8") as config_file:
    cfg = json.load(config_file)

df = pd.read_csv(cfg["file_path"], sep=";", names=cfg["names"],
                 usecols=cfg["usecols"], skiprows=1)

df = df.replace(to_replace='-',value=0)
df = df.astype({'ignorado': 'float64', 'menosum': 'float64','de1a4': 'float64',
           'de5a9': 'float64','de10a14': 'float64','de15a19': 'float64',
           'de20a29': 'float64','de30a39': 'float64','de40a49': 'float64',
           'de50a59': 'float64','60mais': 'float64','total': 'float64'})

df['codmunicipio'] = df.muni.apply(lambda x: x[:6])
df['uf'] = df.muni.apply(lambda x: x[-2:] if "ignorado" in x
                         else('ZZ' if'Ignorado' in x else ''))

df['municipio'] = df.muni.apply(lambda x: x[7:] if 'ignorado' not in x.lower() else '')

df = df.drop(columns=['muni'])

from elasticsearch import Elasticsearch
es = Elasticsearch()

for linha in range(len(df)):
    data = df.loc[linha].to_dict()
    data.update(cfg['padrao'])
    es.index(index=cfg['index'], id=linha, body=data)