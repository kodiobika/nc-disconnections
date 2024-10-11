import pandas as pd
from datetime import datetime
import numpy as np
import json
from pandas.api.types import is_string_dtype
from matplotlib import pyplot as plt
import plotly.express as px

def get_disconnection_data(start_year=None, end_year=None):
    df = pd.read_csv(
        "data/nc_electric_disconnections_1996_2023.csv", parse_dates=['timestamp']
    )
    if start_year:
        df = df.loc[df.timestamp.dt.year >= start_year]
    if end_year:
        df = df.loc[df.timestamp.dt.year <= end_year]
    
    df_out = df.copy().reset_index(drop=True)
    return df_out


def plot_disconnection_time_series(df, start_year=None, end_year=None):
    df_plot = df.copy()
    if start_year:
        df_plot = df_plot.loc[df_plot.timestamp.dt.year >= start_year]
    if end_year:
        df_plot = df_plot.loc[df_plot.timestamp.dt.year <= end_year]

    fig = px.line(
        df_plot.sort_values(["utility_name", "timestamp"]),
        x="timestamp",
        y="disconnections",
        color="utility_name",
        title="Total Disconnections",
        color_discrete_map={
            "Dominion Energy": "orange",
            "Duke Energy Carolinas": "blue",
            "Duke Energy Progress": "green",
            "All Utilities": "red"
        }
    )

    ticks = df_plot.timestamp.dt.year.unique()
    fig.update_xaxes(
        ticks="outside",
        tickmode='array',
        tickvals=ticks,
        ticktext=np.where(ticks.astype(int) % 4 == 0, ticks, " "),
    )
    
    fig.update_layout(
        xaxis_tickformat = '%b <br>%Y',
        yaxis=dict(tickformat=','),
        font_family="Arial",
        legend_title_text="Utility Provider",
        margin=dict(l=100, b=100, r=100, t=100)
    )
    fig.update_yaxes(title='Accounts Disconnected', ticks="outside", automargin=True)
    fig.update_xaxes(title='Reporting Period', ticks="outside")
    
    fig.show()


def plot_disconnection_rate_time_series(df, start_year=None, end_year=None):
    df_plot = df.copy()
    if start_year:
        df_plot = df_plot.loc[df_plot.timestamp.dt.year >= start_year]
    if end_year:
        df_plot = df_plot.loc[df_plot.timestamp.dt.year <= end_year]

    fig = px.line(
        (
            df_plot.loc[df_plot.disconnection_rate < float('inf')]
            .sort_values(["utility_name", "timestamp"])
            .dropna(subset="disconnection_rate")
        ),
        x="timestamp",
        y="disconnection_rate",
        color="utility_name",
        title="Disconnection Rate",
        color_discrete_map={
            "Dominion Energy": "orange",
            "Duke Energy Carolinas": "blue",
            "Duke Energy Progress": "green",
            "All Utilities": "red"
        }
    )
    ticks = df_plot.timestamp.dt.year.unique()
    fig.update_xaxes(
        ticks="outside",
        tickmode='array',
        tickvals=ticks,
        ticktext=np.where(ticks.astype(int) % 2 == 0, ticks, " "),
    )
    
    fig.update_layout(
        xaxis_tickformat = '%b <br>%Y',
        yaxis=dict(tickformat=','),
        font_family="Arial",
        legend_title_text="Utility Provider",
        margin=dict(l=100, b=100, r=100, t=100)
    )
    fig.update_yaxes(title='Disconnection Rate', ticks="outside")
    fig.update_xaxes(title='Reporting Period', ticks="outside")
    
    fig.show()