#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to organize and track trading templates

@author: sbhargava
"""
from . import blueprint
from ..positions.positions import get_positions
from ..positions.risk_report import _create_id_
from ..dash_utils import apply_layout_with_auth
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_html_components as html
import dash_core_components as dcc
from flask import current_app
from flask import request
from pathlib import Path
import datetime as dt
from dash import Dash
import pandas as pd
import numpy as np
import dash_table, json, os, re

url_base = '/dash/Summary/'
base_path = Path(blueprint.root_path, 'data') # Path where trading templates are stored in JSON format

def Add_Dash(server):

    # Define the dash app
    external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = Dash(server=server, url_base_pathname = url_base, external_stylesheets=external_stylesheets)

    # Define the HTML layout of the page
    # Four columns with contract name as a link directing to the trading logic for that contract (app generated in algo.py)
    layout = html.Div([
        dcc.Location(id='url', refresh = False),

        dcc.Store(id = 'position-df'),
        dcc.Store(id = 'algo-exists'),

        html.Div([
            html.Div(
                [html.H5('Recently Updated', style = {'textAlign' : "left", 'color' : '#425270', 'font-weight' : '600', 'font-variant' : 'small-caps'}),
                html.Div(id = 'Recently-Updated')],
                className = 'three columns'
            ),
            html.Div(
                [html.H5('Position On/Missing Algo', style = {'textAlign' : "left", 'color' : '#425270', 'font-weight' : '600', 'font-variant' : 'small-caps'}),
                html.Div(id = 'Position-On-Missing-Algo')],
                className = 'three columns'
            ),

            html.Div(
                [html.H5('Algo Exists/No Position On (RP > 7)', style = {'textAlign' : "left", 'color' : '#425270', 'font-weight' : '600', 'font-variant' : 'small-caps'}),
                html.Div(id = 'Algo-Exists-No-Position')],
                className = 'three columns'
            ),

            html.Div(
                [html.H5('Position On/Incomplete Algo', style = {'textAlign' : "left", 'color' : '#425270', 'font-weight' : '600', 'font-variant' : 'small-caps'}),
                html.Div(id = 'Position-On-Incomplete-Algo')],
                className = 'three columns'
            ),
        ], className = 'row' )
    ])

    # Define Dash app to work with the authorizations of Flask app
    apply_layout_with_auth(app, layout)

    # ----------------------------------------------------------------------------------------------------------------------------- #
    @app.callback(
    [Output('position-df', 'data'),
    Output('algo-exists', 'data')],
    [Input('url', 'pathname')])
    def get_data_files(url):
    # Get in live data on page refresh (live trading positions and current list of contracts for which trading logic is defined)
    # Save the data in a dcc.Store element for other callbacks to use
        if url is None:
            raise PreventUpdate

        # Get position df
        df = get_positions()
        df['id'] = df.apply(lambda x : _create_id_(x), axis=1)
        df['split'] = df.id.str.split("_", 1)
        df['contract'] = df.contract.str.replace("_", " ").str.lower()

        # Get list of algo_exists
        algo_exists = []
        base_directories = os.listdir(base_path)
        for dirs in base_directories:
            prod_dir = Path(base_path, dirs)
            if os.path.isdir(prod_dir):
                 algo_exists.extend(
                 [ '_'.join([dirs.upper(), x.replace('.heuristic', '')]) for x in os.listdir(prod_dir) if re.search('heuristic', x, re.IGNORECASE)]
                 )


        return df.to_json(orient='records'), algo_exists

    # ----------------------------------------------------------------------------------------------------------------------------- #
    @app.callback(
    Output('Position-On-Missing-Algo', 'children'),
    [Input('position-df', 'data'),
    Input('algo-exists', 'data')])
    def position_on_missing_algo(df, algo_exists):
        if not df or not algo_exists:
            return []

        df = pd.DataFrame(json.loads(df))

        table = df[~df['id'].isin(algo_exists)].reset_index()
        table['link'] = table.id.map(lambda x : generate_algo_link(x))

        return generate_table(table, rp = '')

    # ----------------------------------------------------------------------------------------------------------------------------- #
    @app.callback(
    Output('Algo-Exists-No-Position', 'children'),
    [Input('position-df', 'data'),
    Input('algo-exists', 'data')])
    def position_on_missing_algo(df, algo_exists):
        if not df or not algo_exists:
            return []

        df = pd.DataFrame(json.loads(df))
        # Get dataframe of RP from pickle and convert to dict
        fpath = Path(blueprint.root_path, 'data', 'daily_rp').with_suffix('.pkl')
        rp_df = pd.read_pickle(fpath)
        rp_dict = rp_df.to_dict('series')['RP']

        table = pd.DataFrame()
        table['id'] = list(set(algo_exists) - set(df.id.values.tolist())) # Contracts for which positions exists but no algo
        table['link'] = table.id.map(lambda x : generate_algo_link(x))
        table['rp'] = table.id.str.lower().str.replace("_", " ").str[:-2]
        table['rp'] = table.rp.map(lambda x : rp_dict.get(x, 0)).replace("", 0)
        # Only display contracts for which RP > 7
        table = table[((table.rp > 7) & (table.id.str[-1] == 'S')) | ((table.rp < -7) & (table.id.str[-1] == 'B'))]

        return generate_table(table)
    # ----------------------------------------------------------------------------------------------------------------------------- #
    @app.callback(
    Output('Position-On-Incomplete-Algo', 'children'),
    [Input('position-df', 'data')])
    def position_on_missing_algo(df):
        if not df:
            return []

        df = pd.DataFrame(json.loads(df))
        df['incomplete algo'] = df.split.map(lambda x : check_incomplete_algo(x))
        table = df[df['incomplete algo']].reset_index()
        table['link'] = table.id.map(lambda x : generate_algo_link(x))

        return generate_table(table, rp='')
    # ----------------------------------------------------------------------------------------------------------------------------- #
    @app.callback(
    Output('Recently-Updated', 'children'),
    [Input('algo-exists', 'data')])
    def sort_recently_updated(algo_exists):
        if not algo_exists:
            return []

        table = pd.DataFrame()
        table['id'] = algo_exists
        table['link'] = table.id.map(lambda x : generate_algo_link(x))
        table['date'] = table.id.map(lambda x: recently_updated(x.split("_", 1)))
        table = table.sort_values(by='date', ascending = False)
        table = table[table.date > dt.datetime.now() - dt.timedelta(10)]
        table['date'] = table.date.dt.strftime('%a %I:%S %p')

        return generate_table(table, rp=['date'])

    return app.server
# -------------------------------------------------------------------------------------------------------------------------------------------------------- #
def recently_updated(x):
    prod, rel = x
    filepath = Path(base_path, prod.capitalize(), rel).with_suffix('.heuristic')

    file_json = json.load(open(Path(filepath), "r"))
    algo_df = pd.DataFrame(file_json).set_index('Params')
    return dt.datetime.strptime(algo_df.loc['Last Updated', 'Value'], "%b %d %Y %X")


def check_incomplete_algo(x):
    prod, rel = x
    filepath = Path(base_path, prod.capitalize(), rel).with_suffix('.heuristic')
    ret = True

    if os.path.isfile(filepath):
        file_json = json.load(open(Path(filepath), "r"))
        algo_df = pd.DataFrame(file_json).set_index('Params')
        algo_df.drop(labels = ['', 'Standard Deviation', 'Risk', 'Unwind Position'], axis = 0, inplace=True)

        if algo_df[algo_df.Value == ''].empty:
            ret = False

    return ret

def generate_algo_link(x):
    prod = x.split("_")[0].lower()
    contract_url = '/'.join(['dash', prod,x])
    return ''.join([request.url_root, contract_url])


def generate_table(dataframe, name = ['id'], link = ['link'], rp = ['rp']):
    dataframe.id = dataframe.id.str.replace("_", " ")

    if rp == '':
        a = html.Table(
            [html.Tr([
                html.Td( [html.A(dataframe.iloc[i][n], href=dataframe.iloc[i][l] , style = {'text-decoration' : 'none'})] ) for n, l in zip(name, link)
            ]) for i in range(len(dataframe))]
        )
    else:
        a = html.Table(
            [html.Tr([
                html.Td( [html.A(dataframe.iloc[i][n], href=dataframe.iloc[i][l] , style = {'text-decoration' : 'none'}), '    ', dataframe.iloc[i][r]] ) for n, l, r in zip(name, link, rp)
            ]) for i in range(len(dataframe))]
        )
    return a
