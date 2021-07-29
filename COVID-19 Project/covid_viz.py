#Link to Jupyter Notebook hosted on IBM:
#Import necessary libraries
import ibm_db
import ibm_db_sa
import sqlalchemy
import plotly.offline as pyo      #Optional
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
pyo.init_notebook_mode()          #Optional
!conda install -c conda-forge folium=0.5.0 --yes
import folium
import json
import requests 
import numpy as np

#Connect to Db2 and load SQL magic
%load_ext sql
%sql ibm_db_sa://lhx60203:c4n@********@dashdb-txn-sbox-yp-dal09-10.services.dal.bluemix.net:50000/BLUDB

#SQL Queries
%%sql global_death_percent <<
SELECT SUM(NEW_CASES) AS TOTAL_GLOBAL_CASES, SUM(NEW_DEATHS) AS TOTAL_GLOBAL_DEATHS, SUM(CAST(NEW_DEATHS AS FLOAT))/SUM(CAST(NEW_CASES AS FLOAT))*100 AS DEATH_PERCENTAGE
  FROM COVID_DEATH_DATA
  WHERE CONTINENT IS NOT NULL

%%sql total_death_count <<
SELECT LOCATION, SUM(NEW_DEATHS) AS TOTAL_DEATH_COUNT
  FROM COVID_DEATH_DATA
  WHERE CONTINENT IS NULL and LOCATION not in ('World', 'European Union', 'International') 
  GROUP BY LOCATION
  ORDER BY TOTAL_DEATH_COUNT DESC NULLS LAST

%%sql highest_infection_count <<
SELECT LOCATION, POPULATION, MAX(TOTAL_CASES) AS HIGHEST_INFECTION_COUNT, MAX((CAST(TOTAL_CASES AS FLOAT)/CAST(POPULATION AS FLOAT)))*100 AS POPULATION_INFECTED_PERCENT
  FROM COVID_DEATH_DATA
  GROUP BY LOCATION, POPULATION
  ORDER BY POPULATION_INFECTED_PERCENT DESC NULLS LAST

%%sql highest_infection_rolling <<
SELECT LOCATION, POPULATION, DATE, MAX(TOTAL_CASES) AS HIGHEST_INFECTION_COUNT, MAX((CAST(TOTAL_CASES AS FLOAT)/CAST(POPULATION AS FLOAT)))*100 AS POPULATION_INFECTED_PERCENT
  FROM COVID_DEATH_DATA
  WHERE LOCATION IN ('United States', 'India', 'Brazil', 'Russia', 'United Kingdom')
  GROUP BY LOCATION, POPULATION, DATE
  ORDER BY POPULATION_INFECTED_PERCENT DESC NULLS LAST

%%sql percent_pop_vaccinated <<
SELECT *
  FROM PERCENT_POP_VAXED
  WHERE LOCATION IN ('United States', 'India', 'Brazil', 'Russia', 'United Kingdom')

#Convert SQL Queries to DataFrames
df_gdp = global_death_percent.DataFrame()
df_tdc = total_death_count.DataFrame()
df_hic = highest_infection_count.DataFrame()
df_hir = highest_infection_rolling.DataFrame()
df_ppv = percent_pop_vaccinated.DataFrame()

#Data Cleaning
df_hic.fillna(0, inplace=True)
df_hir.fillna(0, inplace=True)
df_hic.fillna(0, inplace=True)
df_ppv.fillna(0, inplace=True)


#Figure 1
gdp_fig = go.Figure(data=[go.Table(
    header=dict(values=list(['Total Global Cases', 'Total Global Deaths', 'Death Percentage (%)']),
                line_color= 'darkslategray',
                fill_color='lightskyblue',
                align='left'),
    cells=dict(values=[df_gdp.total_global_cases, df_gdp.total_global_deaths, df_gdp.death_percentage.map(u'{:,.2f}'.format)],
               fill_color='lavender',
               align='left'))
])
gdp_fig.update_layout(title='Death Percentage Worldwide', title_x=0.5, width=650, height=450)
gdp_fig.show()

#Figure 2
tdc_fig = px.bar(df_tdc, x=df_tdc['location'], y=df_tdc['total_death_count'], title='Total Death Count')
tdc_fig.update_layout(xaxis_title='Continent', yaxis_title='Total Death Count', title_x = 0.5)
tdc_fig.update_traces(marker_color='lightskyblue', marker_line_color='darkblue')
tdc_fig.show()

#Percent of Population Infected
#Download countries GeoJSON file
response = requests.get('https://raw.githubusercontent.com/obeykhadija/Data_Analyst_Portfolio/624951347ff65cf77394da777dc7fe6a8b958f0c/world_countries.json')
world_geo = response.json()

#Create plain world map
world_map = folium.Map(location=[0, 0], zoom_start=2)

#Figure 3
#Define Threshold Scale
threshold_scale = np.linspace(df_hic['population_infected_percent'].min(),
                              df_hic['population_infected_percent'].max(),
                              6, dtype=int)
threshold_scale = threshold_scale.tolist()                   
threshold_scale[-1] = threshold_scale[-1] + 1

#Create Choropleth Map
world_map.choropleth(
    geo_data=world_geo,                                      #GeoJSON file
    data=df_hic,                                             #Highest_Infection_Count DataFrame     
    columns=['location', 'population_infected_percent'],     #Columns in the df that will be used
    key_on='feature.properties.name',                        #the key or variable in the GeoJSON file that contains the name of the variable of interest
    threshold_scale=threshold_scale,
    fill_color='PuBuGn', 
    fill_opacity=0.7, 
    line_opacity=0.2,
    legend_name='Population Infected with COVID-19 (%)',
    reset=True
)

#Display Choropleth Map
world_map

#Rolling Infection Count
df_hir['location'].value_counts()

#Figure 4
hir_fig = px.line(df_hir, x='DATE', y='highest_infection_count', color='location')
hir_fig.update_layout(title='Infection Count', xaxis_title='Date (Month)', yaxis_title='Infection Count', title_x=0.5)

hir_fig.show()

#Percent of Population Vaccinated
ppv_fig = px.line(df_ppv, x='DATE', y='rolling_ppl_vaccinated', color='location')
ppv_fig.update_layout(title='Total People Vaccinated', xaxis_title='Date (Month)', yaxis_title='Vaccinations', title_x=0.5)

hir_fig.show()
