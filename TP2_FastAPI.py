# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 10:17:18 2023

@author: Sam
"""

from os import path
from pathlib import Path

import pandas as pd
import plotly.express as px
import numpy as np
import streamlit as st

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

import matplotlib.pyplot as plt

path = "C:/Users/Sam/Desktop/data"
df_impressions = pd.read_csv("{}/impressions.csv".format(path))

# convert timestamp to date and create new column named 'date_impression'
df_impressions['date_impression'] = pd.to_datetime(df_impressions['timestamp'], unit='s')
print(f"Data impressions shape: {df_impressions.shape}")

df_clics = pd.read_csv("{}/clics.csv".format(path))

# convert timestamp to date and create new column named 'date_clic'
df_clics['date_clic'] = pd.to_datetime(df_clics['timestamp'], unit='s')
print(f"Data clics shape: {df_clics.shape}")

df_achats = pd.read_csv("{}/achats.csv".format(path))


# convert timestamp to date and create new column named 'date_achat'
df_achats['date_achat'] = pd.to_datetime(df_achats['timestamp'], unit='s')
print(f"Data achat shape: {df_achats.shape}")


#merge dataset

df = (df_impressions
        # add clic on impressions that have this action
        .merge(df_clics.drop("timestamp", axis=1), how="left", on="cookie_id")
        # add clic on impressions that have this action
        .merge(df_achats.drop("timestamp", axis=1), how="left", on="cookie_id")
        .assign(is_clic=lambda dfr: dfr.date_clic.notnull(),
                is_achat=lambda dfr: dfr.date_achat.notnull(),
               )
       )

#API




app = FastAPI()


@app.get("/dataframe", response_class=JSONResponse)
async def get_data():
    # Convertir les objets Timestamp, NaT et valeurs float en chaînes de texte pour la sérialisation JSON
    df_json = df.replace({pd.NaT: None}).applymap(lambda x: str(x) if isinstance(x, (pd.Timestamp, float)) else x).to_dict(orient='records')

    return JSONResponse(content=df_json)

# Nouvel endpoint pour obtenir les données relatives à une campagne spécifique
@app.get("/get_data_campaign/{campaign_id}", response_class=JSONResponse)
async def get_data_campaign(campaign_id: int):
    # Filtrer le DataFrame pour obtenir les données relatives à la campagne spécifiée
    campaign_df = df[df['campaign_id'] == campaign_id]

    if campaign_df.empty:
        raise HTTPException(status_code=404, detail="Campagne non trouvée")

    # Convertir les objets Timestamp, NaT et valeurs float en chaînes de texte pour la sérialisation JSON
    campaign_json = campaign_df.replace({pd.NaT: None}).applymap(lambda x: str(x) if isinstance(x, (pd.Timestamp, float)) else x).to_dict(orient='records')

    return JSONResponse(content=campaign_json)

#TP2 dashboard streamlit - q. 1,2,3 et 4


campaign_metrics = df.groupby('campaign_id').agg({
    'price': 'sum',
    'is_clic': 'sum',
    'is_achat': 'sum'
}).reset_index()

# Application Streamlit
st.title("Tableau de bord - Chiffre d'affaires, Nombre d'affichages, Nombre de clics, et Nombre d'achats par Campagne")

# Tableau de chiffre d'affaires, nombre total de clics, nombre total d'affichages des bannières, et nombre d'achats par campagne
st.write("### Chiffre d'affaires, Nombre d'affichages, Nombre de clics, et Nombre d'achats par Campagne")
st.dataframe(campaign_metrics)

# Graphique à barres pour visualiser le chiffre d'affaires par campagne
st.write("### Graphique - Chiffre d'affaires par Campagne")
st.bar_chart(campaign_metrics.set_index('campaign_id')['price'])

# Graphique à barres pour visualiser le nombre d'affichages des bannières par campagne
st.write("### Graphique - Nombre d'affichages par Campagne")
st.bar_chart(campaign_metrics.set_index('campaign_id')['is_clic'])

# Graphique à barres pour visualiser le nombre d'achats par campagne
st.write("### Graphique - Nombre d'achats par Campagne")
st.bar_chart(campaign_metrics.set_index('campaign_id')['is_achat'])

# Répondre à la question spécifique
st.write("### Réponse aux questions :")
st.write("Le chiffre d'affaires généré par campagne, le nombre d'affichages des bannières, le nombre de personnes qui viennent sur le site web (nombre de clics), et le nombre d'achats sont présentés dans les graphiques ci-dessus.")
