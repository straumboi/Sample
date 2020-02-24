#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interactive app to define trading logic based on data entered by user using Flask/Plotly Dash/SQLAlchemy/pandas/numpy
Celery process to download daily open interest from CME website and upload to the database

@author: sbhargava
"""
# Relative imports from Flask website
from . import blueprint
from ..positions import positions
from ..dash_utils import apply_layout_with_auth
from .google_invite import google_calendar_invite
from ..base.models import IntraDayPositions, TickerData


from dash.dependencies import Input, Output, State
import dash_table, itertools, time, json, os, re
from dash.exceptions import PreventUpdate
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime, date
from dash.dash import no_update
from pathlib import Path
from dash import Dash
import pandas as pd
import numpy as np

# Making a change to any of these variables will require restarting the server
# Define all products and relationships traded. Also define 'round' value based on tick value sig digits
pdict = {
    'Brent': {'round': 2, 'rel': ['1m Fly', '1m 2x', '1m DC', '2m Fly', '2m 2x', '3m Fly', '3m 2x']},
    'Brent_6m': {'round': 2, 'rel': ['6m Fly', '6m 2x', '12m Fly', '12m 2x']},
    'Brent_crack': {'round': 2, 'rel': ['1m Crack', '2m Crack', '3m Crack', '6m Crack', '12m Crack']},
    'Cocoa': {'round': 0, 'rel': ['consecutive fly', 'consecutive 2x']},
    'Cocoaliffe': {'round': 0, 'rel': ['consecutive fly', 'consecutive 2x']},
    'Cocoa-cocoaliffe': {'round': 0, 'rel': ['consecutive sp']},
    'Feedercattle': {'round': 3, 'rel': ['consecutive Fly', 'consecutive 2x']},
    'Leanhogs': {'round': 3, 'rel': ['consecutive Fly', 'consecutive 2x']},
    'Livecattle': {'round': 3, 'rel': ['2m Fly', '2m 2x']},
    'Go': {'round': 2, 'rel': ['1m 2x', '2m 2x', '3m Fly', '3m 2x', '6m Fly', '6m 2x', '12m Fly', '12m 2x']},
    'Ho': {'round': 0, 'rel': ['1m Fly', '1m 2x', '2m Fly', '2m 2x', '3m Fly', '3m 2x']},
    'Ho-go': {'round': 2, 'rel': ['1m Sp', '2m Sp', '3m Sp', '6m Sp', '12m Sp']},
    'Naturalgas': {'round': 3, 'rel': ['1m Fly', '1m 2x', '2m Fly', '2m 2x', '12m 2x']},
    'Gasoline(rbob)': {'round': 0, 'rel': ['1m Fly', '1m 2x', '2m Fly', '2m 2x', '3m Fly', '3m 2x']},
    'Soybeanoil': {'round': 2, 'rel': ['consecutive Fly', 'consecutive 2x']},
    'Sugarno.11': {'round': 2, 'rel': ['consecutive Fly', 'consecutive 2x']},
    'Wheat': {'round': 2, 'rel': ['consecutive Fly', 'consecutive 2x']},
    'Kcwheat': {'round': 2, 'rel': ['consecutive Fly', 'consecutive 2x']},
}

# Define the dash app server to add to the flask website
def Add_Dash(server):

    external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css']
    assets_folder = Path(blueprint.root_path, 'assets')
    app = Dash(server=server, url_base_pathname = '/dash/', external_stylesheets=external_stylesheets, assets_folder = assets_folder)

    app.config.suppress_callback_exceptions = True

    # Define the HTML elements
    layout = html.Div([

        # Storage elements for data that needs to be updated only on page refresh
        dcc.Store(id = 'risk-report'),
        dcc.Store(id = 'all-rp'),

        # Main table that generates other tables
        main_table_html(),

        dcc.Location(id='url', refresh = False),

        # Contract name and settings
        html.Div([
            html.H6(
                id = 'contract-name',
                style={"textAlign" : "left", "color" : "#425270", "font-weight" : "600"}
            ),
        ], className = 'row'),

        html.Div(daily_rp_html(id = 'settle-rp')),

        html.Div([
            # Trading data input table
            html.Div(
                id = 'heuristic-div',
                children = [baseline_heuristic_html(id = 'base-heuristic'),
                html.Br(),
                html.Button('Save', id='Save-heuristic', style = {'background-color' : '#dfe1eb'}),
                # Google calandar invite option to send reminders
                html.Div(
                    id='cal-invite-div',
                    children = [
                        html.Div(
                            html.P('Google Calendar Invite', style={"color" : "#425270", "font-weight" : "600", 'font-variant' : 'small-caps', 'margin-top' : '20px'})
                        ),
                        dcc.Input(
                            id='invite-title',
                            placeholder = 'Event Name',
                            type = 'text',
                            value='',
                            style={'width':'100%'}
                        ),
                        dcc.DatePickerSingle(
                            id='invite-date',
                            placeholder='MM/DD/YYYY',
                        ),
                        html.Button(
                            id='invite-submit',
                            n_clicks_timestamp=0,
                            children='Send',
                            style = {'background-color' : '#dfe1eb'}
                        ),
                        html.Div(id='invite-link')]
                )],
                style = {'display' : 'none'}, className = 'two columns'
            ),

            html.Div([
                # Displays summarized data of how much to trade at each price level
                tiers_table_html(id = 'add-unwind-parameters'),
                # Displays notes box for traders to enter any notes on changes made/when/why
                html.Div(
                    id = 'notes-div',
                    children = [notes_html(id = 'notes'),
                    html.Br(),
                    html.Button('Save', id='Save-notes', style = {'background-color' : '#dfe1eb'})],
                    style = {'display' : 'none'}),
                ],
                className = 'four columns'
            ),

            # Displays detailed chart on how to add on a position for trading
            html.Div(
                [html.P(id = 'adding-chart-title', style={"color" : "#425270", "font-weight" : "600", 'font-variant' : 'small-caps'}),
                chart_html(id = 'adding-chart')],
                className = 'three columns'
            ),

            # Displays detailed chart on how to unwind the position
            html.Div(
                [html.P(id = 'unwinding-chart-title', style={"color" : "#425270", "font-weight" : "600", 'font-variant' : 'small-caps'}),
                chart_html(id='unwinding-chart')],
                className = 'three columns'
            )
        ], className = 'row' ),

        # Dropdown menus for postmortem analysis of expired trade contracts
        html.Footer([
            html.Div(
                html.P('Archive'),
                className = 'one columns',
                style={"textAlign" : "center", "color" : "#425270", "font-weight" : "600", 'font-variant' : 'small-caps'}
            ),
            dcc.Dropdown(
                id = 'month-year-ddown',
                placeholder = 'Select Month',
                className = 'two columns'
            ),

            dcc.Dropdown(
                id = 'relationship-ddown',
                placeholder = 'Select Relationship',
                className = 'three columns'
            ),
            html.Button(
                id='ddown-submit',
                n_clicks_timestamp=0,
                children='Submit',
                style = {'background-color' : '#dfe1eb'}
            )
        ], className = 'row', style = {'margin-top' : '20px'}),

    ])

    # Add Dash app to work within the authorizations of the flask website
    apply_layout_with_auth(app, layout)

    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('risk-report', 'data'),
    Output('main-table', 'data'),
    Output('main-table', 'columns'),
    Output('main-table', 'tooltip_data'),
    Output('main-table', 'style_data_conditional'),
    Output('main-table', 'active_cell')],
    [Input('url', 'pathname')])
    def create_maintbl_actvcell(url):
        # Stores position df for current product in dcc.Store element
        # Initializes the main table for the given product read through URL(months, relationship, position)
        if url is None:
            raise PreventUpdate

        # Get product selected from URL and account for exceptions.
        # Account for naming convention anomalies present in legacy system
        prod_lookup = url.split('/')[2].capitalize()
        dur = ''
        if re.search('rbob', prod_lookup, re.IGNORECASE):
            prod_lookup = 'Gasoline(rbob)'
        if re.search('6', prod_lookup, re.IGNORECASE):
            dur = '_6m'
        prod = prod_lookup.split("_")[0]

        pos_df, columns, data, tooltip, style = init_main_table(pdict[prod_lookup]['rel'], prod, dur)
        contract = url.split('/')[-1]

        # If URL contains a specific contract in addition to product, set that as the active cell
        if contract:
            prod2, dur, rel, mmyy, b_s = contract.split("_")
            active_cell = {'column_id': ' '.join([mmyy, b_s]), 'row_id': ' '.join([prod.capitalize(), dur, rel])}
            return pos_df.to_json(orient='records'), data, columns, tooltip, style, active_cell
        else:
            return pos_df.to_json(orient='records'), data, columns, tooltip, style, no_update

    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('month-year-ddown', 'options')],
    [Input('url', 'pathname')])
    def set_mmyy_dropdown(url):
        # Set option for month year dropdown for archived trades for selected product
        if url is None:
            raise PreventUpdate

        prod = url.split('/')[2].capitalize()
        files = update_dropdown(prod)

        mmyy = [x.split("_")[2] for x in files]
        mmyy =  list(set(mmyy))

        mmyy = [{'label' : i, 'value' : i} for i in mmyy]

        return [mmyy]
    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('relationship-ddown', 'options')],
    [Input('month-year-ddown', 'value'),
    Input('url', 'pathname')])
    def set_relationship_dropdown(mmyy, url):
        # Get month year and prod to set relationship dropdown for archived trades for selected product
        if (mmyy is None) or (url is None):
            raise PreventUpdate

        prod = url.split('/')[2].capitalize()
        files = update_dropdown(prod)

        rel = [x for x in files if re.search(mmyy, x, re.IGNORECASE)]
        rel = [' '.join([x.split("_")[0], x.split("_")[1], x.split("_")[3]]) for x in rel]
        rel = list(set(rel))

        rel = [{'label' : i, 'value' : i} for i in rel]

        return [rel]
    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('contract-name', 'children')],
    [Input('main-table', 'active_cell'),
    Input('ddown-submit', 'n_clicks_timestamp'),
    Input('url', 'pathname')],
    [State('month-year-ddown', 'value'),
    State('relationship-ddown', 'value')])
    def get_contract(active_cell, ts, url, mmyy, rel):
        # Set the current contract either from archive or active cell
        tnow = int(str(time.time())[:10])

        if (not url) or ((not active_cell) and (ts == 0)):
            raise PreventUpdate

        if tnow == int(str(ts)[:10]):
            if re.search('crack', url, re.IGNORECASE):
                prod = 'Brent'
            elif re.search('brent', url, re.IGNORECASE) and re.search('6', url, re.IGNORECASE):
                prod = 'Brent'
            elif re.search('rbob', url, re.IGNORECASE):
                prod = 'Gasoline(rbob)'
            else:
                prod = url.split('/')[2].capitalize()

            contract_name = ' '.join([prod, rel[:-2], mmyy, rel[-1]])
        elif active_cell['column_id'] == 'Future':
            raise PreventUpdate
        else:
            contract_name = ' '.join([active_cell['row_id'], active_cell['column_id']])

        return [contract_name]

    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('settle-rp', 'columns'),
    Output('settle-rp', 'data')],
    [Input('contract-name', 'children')])
    def show_settle_data(contract_name):
        # Show settle data (rp, settle price, mean range, median range) from daily RP script
        # (data pulled from legacy process in the form of a pickle file)

        if (not contract_name):
            raise PreventUpdate

        contract_name = contract_name[:-2].lower()
        fpath = Path(blueprint.root_path, 'data', 'daily_rp').with_suffix('.pkl')
        df = pd.read_pickle(fpath)
        df.rename(columns = {"Price" : "Settle Price"}, inplace = True)

        columns = [{"name": i, "id": i} for i in df.columns]
        try:
            data = [df.loc[contract_name].to_dict()]
        except:
            data = []
            columns = []

        return columns, data

    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('base-heuristic', 'columns'),
    Output('base-heuristic', 'data'),
    Output('heuristic-div', 'style'),
    Output('base-heuristic', 'data_previous')],

    [Input('base-heuristic', 'data_timestamp'),
    Input('contract-name', 'children'),
    Input('main-table', 'active_cell'),
    Input('risk-report', 'data'),
    Input('Save-heuristic', 'n_clicks_timestamp')],

    [State('base-heuristic', 'data_previous'),
    State('base-heuristic', 'data')])
    def generate_heuristic_table(ts, contract_name, active_cell, risk_json, ts_save, rows_previous, rows):
    # Gets input from the selected contract (active cell in the main table) and generates a table for traders to enter
    # data into. This data is used later on for calculations.
        tnow = int(str(time.time())[:10])

        if (not contract_name):
            raise PreventUpdate

        prod, rel = contract_name.split(" ", 1)
        add, unwind = get_add_unwind_type(contract_name[-1])

        if re.search('crack', rel, re.IGNORECASE): #For p_dict lookup
            prod_lookup = 'Brent_crack'
        elif re.search('rbob', prod, re.IGNORECASE):
            prod_lookup = 'Gasoline(rbob)'
        else:
            prod_lookup = prod.capitalize()

        str_format = lambda x:"{:,.{}f}".format(x, pdict[prod_lookup]['round'])

        # Get data from risk report to calculate std and position
        pos_df = pd.DataFrame([[contract_name[:-2].replace(' ', '_'), 0]], columns = ['contract', 'position'])

        # Try to get standard deviation data if available otherwise leave blank
        # (data generation depends on legacy process and is sometimes not available)
        try:
            risk = positions.get_risk_report(pos_df)
            std = risk['std'][0]
            std_str = str_format(std)
        except:
            std = ''
            std_str = ''
            scalp_str = ''

        # Get the current position for contract if it exists else set to 0
        try:
            risk_df = pd.DataFrame(json.loads(risk_json)).set_index('contract')
            temp_pos = int(risk_df.loc[contract_name[:-2].lower(), 'position'])
            if (add == 'Buy' and temp_pos > 0) or (add=='Sell' and temp_pos < 0):
                curr_pos = temp_pos
            else:
                curr_pos = 0
        except:
            curr_pos = 0

        columns = [
            {'name' : '{} Side Heuristic'.format(add), 'id' : 'Params', 'editable' : False},
            {'name' : '', 'id' : 'Value', 'editable' : True}
        ]

        base_path = Path(blueprint.root_path, 'data/{}'.format(prod))
        filepath = Path(base_path, rel.replace(' ', '_')).with_suffix('.heuristic')
        filepath_exp = Path(base_path, 'expired', rel.replace(' ', '_')).with_suffix('.heuristic')

        # ------------- First check if save button is pressed ----------------------- #
        if rows and isinstance(ts_save, int) and tnow == int(str(ts_save)[:10]):
            rows_df = pd.DataFrame(rows).set_index('Params')
            rows_df.loc['Last Updated', 'Value'] = datetime.now().strftime('%h %d %Y %X')
            data = rows_df.reset_index().to_dict('records')
            json.dump(data, open(filepath, "w+"))
        # ----- Check if it is a row update and calculations need to be redone ------ #
        elif (rows and rows_previous) and rows != rows_previous:
            prev_df = pd.DataFrame(rows_previous).set_index('Params')
            rows_df = pd.DataFrame(rows).set_index('Params')

            # determine whether risk or position needs to be calculated
            change = pd.concat([rows_df,prev_df]).drop_duplicates(keep=False) # Get rows where value changed
            if not change.empty and re.search('risk', change.index[0], re.IGNORECASE):
                rows_df.loc['Max Position', 'Value'] = calculate_pos_risk(rows_df)
            else:
                rows_df.loc['Risk', 'Value'] = calculate_pos_risk(rows_df, 'risk')

            data = rows_df.reset_index().to_dict('records')
        # ----------------- Load file from system if exists ----------------------- #
        elif os.path.isfile(filepath):
            data = json.load(open(filepath, "r"))
            rows_df = pd.DataFrame(data).set_index('Params')

            rows_df.loc['Standard Deviation', 'Value'] =  std #to avoid precision error in calc
            rows_df.loc['Risk', 'Value'] = calculate_pos_risk(rows_df, 'risk')
            rows_df.loc['Standard Deviation', 'Value'] =  std_str

            data = rows_df.reset_index().to_dict('records')
        # ----------------- Load file from archive if exists ----------------------- #
        elif os.path.isfile(filepath_exp):
            data = json.load(open(filepath_exp, "r"))
        # ------------------ Initialize file for first time ------------------------ #
        else:
            if re.search('crack', rel, re.IGNORECASE):
                prod = 'BRENT-CRACK'
            else:
                prod = prod.upper()
            # Get tick data from product from database
            tick_data = TickerData.query.filter_by(shiny_id = prod).first()

            data = [
                {'Params' : 'Max Position', 'Value' : ''},
                {'Params' : 'Standard Deviation', 'Value' : std_str},
                {'Params' : 'Standard Deviation Mult', 'Value' : tick_data.std_mult},
                {'Params' : 'Tick Size', 'Value' : tick_data.custom_tick_size},
                {'Params' : 'Tick Value', 'Value' : tick_data.tick_value},
                #.............. hidden ..................#
                {'Params' : 'Last Updated', 'Value' : ''},
            ]
            data[0]['Value'] = calculate_pos_risk(pd.DataFrame(data).set_index('Params'))

        return columns, data, {}, None


    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('notes', 'value'),
    Output('notes-div', 'style')],
    [Input('contract-name', 'children'),
    Input('Save-notes', 'n_clicks_timestamp')],
    [State('notes', 'value')])
    def generate_notes_table(contract_name, ts, note):
        # Generate a notes table or read in notes based on contract selected
        tnow = int(str(time.time())[:10])

        if not contract_name:
            return [''], {'display' : 'none'}

        prod, rel = contract_name.split(" ", 1)
        base_path = Path(blueprint.root_path, 'data/{}'.format(prod))
        filepath = Path(base_path, rel.replace(' ', '_')).with_suffix('.notes')
        filepath_exp = Path(base_path, 'expired', rel.replace(' ', '_')).with_suffix('.notes')

        if note and isinstance(ts, int) and tnow == int(str(ts)[:10]):
            json.dump(note, open(filepath, "w+"))

        if os.path.isfile(filepath):
            note = json.load(open(filepath, "r"))
        elif os.path.isfile(filepath_exp):
            note = json.load(open(filepath_exp, "r"))
        else:
            note = ['']

        return note, {}

    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('cal-invite-div', 'style'),
    Output('invite-link', 'children')],
    [Input('contract-name', 'children'),
    Input('invite-submit', 'n_clicks_timestamp')],
    [State('invite-title', 'value'),
    State('invite-date', 'date')])
    def calendar_invite(contract_name, ts, title, event_date):
        # Use google calandar API to set up option to send invites by selecting the date and entering a title
        tnow = int(str(time.time())[:10])

        if not contract_name:
            return {'display' : 'none'}, []

        if isinstance(ts, int) and tnow == int(str(ts)[:10]):
            link = google_calendar_invite.main(event_name=title, start_date=datetime.strptime(event_date, '%Y-%m-%d'))
            ret = html.A('Event created here', href=link, target="_blank")
            return {'margin-top' : '40px'}, ret

        return {}, []

    #-------------------------------------------------------------------------------------------------------------#
    @app.callback(
    [Output('add-unwind-parameters', 'columns'),
    Output('add-unwind-parameters', 'data'),
    Output('adding-chart', 'columns'),
    Output('adding-chart', 'data'),
    Output('adding-chart-title', 'children'),
    Output('unwinding-chart', 'columns'),
    Output('unwinding-chart', 'data'),
    Output('unwinding-chart-title', 'children')],

    [Input('add-unwind-parameters', 'data_timestamp'),
    Input('base-heuristic', 'data'),
    Input('risk-report', 'data'),
    Input('contract-name', 'children')],

    [State('add-unwind-parameters', 'data_previous'),
    State('add-unwind-parameters', 'data')])
    def generate_tables_charts(t, heuristic_tbl, risk_json, contract_name, rows_previous, rows):
    # Read in values provided by traders and create trading logic
        tnow = int(str(time.time())[:10])

        if ((not heuristic_tbl) and (not rows)) or (not contract_name):
            return [], [], [], [], [], [], [], []

        prod, rel = contract_name.split(" ", 1)
        add, unwind = get_add_unwind_type(rel[-1])

        if re.search('crack', rel, re.IGNORECASE): #For dict lookup
            prod = 'Brent_crack'
        else:
            prod = prod.capitalize()

        str_format = lambda x:"{:,.{}f}".format(x, pdict[prod]['round'])

        # ------------------------------------------------------------------------------------------------------------------- #
        # STEP 1 : READ IN VALUES FROM HEURISTIC TABLE
        # ------------------------------------------------------------------------------------------------------------------- #
        try:
            # ....... hidden .......#
            # If any of these fail, return empty elements
        except:
            return [], [], [], [], [], [], [], []


        # ------------------------------------------------------------------------------------------------------------------- #
        # STEP 2 : CALCULATE PRICE TIERS FOR ADDING POSITION AND GENERATE CHART
        # ------------------------------------------------------------------------------------------------------------------- #
        try:
            # ....... hidden .......#
            # If any of these fail, return empty elements
        except:
            return [], [], [], [], [], [], [], []

        # ------------------------------------------------------------------------------------------------------------------- #
        # STEP 3 : CREATE SUMMARIZED TIER TABLE FOR ADDING POSITION
        # ------------------------------------------------------------------------------------------------------------------- #
        try:
            # ....... hidden .......#
            # If any of step 3 fails, only return elements from step 2
        except:
            return [], [], chart_columns, adding_chart_data, adding_chart_title, [], [], []

        # ------------------------------------------------------------------------------------------------------------------- #
        # STEP 4 : LOOKUP LOGIC FOR ADDING
        # ------------------------------------------------------------------------------------------------------------------- #
        # This does a function similar to vlookup in excel by looking up position/quantity to be traded at a certain price
        # from the large table generated in step 1

        # If value is entered in the lookup table, lookup value
        if rows and isinstance(t, int) and tnow == int(str(t)[:10]):
            update_df = pd.DataFrame(rows).set_index('Chart')
            orig_df = pd.DataFrame(rows_previous).set_index('Chart')
            try:
                data_df = lookup_logic(
                    chart_df = chart_df,
                    data = data,
                    logic_type = add,
                    update_df = update_df,
                    orig_df = orig_df,
                )
                data = data_df.reset_index().to_dict('records')
            except:
                pass
        # Set default value on initialization,
        # check if contract is currently being traded and has a position on. If yes, display that on lookup
        elif risk_json and json.loads(risk_json):
            try:
                risk_df = pd.DataFrame(json.loads(risk_json)).set_index('contract')
                pos = risk_df.loc[contract_name[:-2].lower(), 'position']
                change = pd.DataFrame([[ 'Adding Lookup', int(pos) ]], columns = ['Chart', 'Position']).set_index('Chart')
                data_df = lookup_logic(
                    chart_df = chart_df,
                    data = data,
                    logic_type = add,
                    change = change
                )
                data = data_df.reset_index().to_dict('records')
            except:
                pass

        # ------------------------------------------------------------------------------------------------------------------- #
        # STEP 5 : GENERATE CHART FOR UNWINDING PARAMS
        # ------------------------------------------------------------------------------------------------------------------- #
        try:
            # ....... hidden .......#
            # If any of these fail, return all other elements upto step 4
        except:
            return columns, data, chart_columns, adding_chart_data, adding_chart_title, [], [], []

        # ------------------------------------------------------------------------------------------------------------------- #
        # STEP 6 : ADD UNWINDING PARAMS TO TIER TABLE
        # ------------------------------------------------------------------------------------------------------------------- #
        # Extends table created in step 3 (summarized tier table) to include logic for unwinding the position as well (if this
        # data is provided by traders in heuristic table)
        try:
            # ....... hidden .......#
            # If any of these fail, return all other elements upto step 5
        except:
            return columns, data, chart_columns, adding_chart_data, adding_chart_title, chart_columns, unwind_chart_data, unwind_chart_title

        # ------------------------------------------------------------------------------------------------------------------- #
        # STEP 7 : LOOKUP UNWINDING LOGIC
        # ------------------------------------------------------------------------------------------------------------------- #
        # Same logic as adding chart lookup
        if rows and isinstance(t, int) and tnow == int(str(t)[:10]):
            try:
                data_df = lookup_logic(
                    chart_df = unwind_chart_df,
                    data = data,
                    logic_type = unwind,
                    update_df = update_df,
                    orig_df = orig_df,
                    run_unwind=True
                )
                data = data_df.reset_index().to_dict('records')
            except:
                pass
        elif risk_json and json.loads(risk_json):
            try:
                risk_df = pd.DataFrame(json.loads(risk_json)).set_index('contract')
                pos = risk_df.loc[contract_name[:-2].lower(), 'position']
                change = pd.DataFrame([['Unwinding Lookup', int(pos)]], columns = ['Chart', 'Position']).set_index('Chart')
                data_df = lookup_logic(
                    chart_df = unwind_chart_df,
                    data = data,
                    logic_type = unwind,
                    change = change,
                    run_unwind=True
                )
                data = data_df.reset_index().to_dict('records')
            except:
                pass


        return columns, data, chart_columns, adding_chart_data, adding_chart_title, chart_columns, unwind_chart_data, unwind_chart_title


    return app.server

#---------------------------------------- HELPER FUNCTIONS ------------------------------------------------------ #
def lookup_logic(chart_df, data, logic_type, update_df='', orig_df='', run_unwind=False, change=pd.DataFrame()):

    if change.empty: # If lookup is manually edited
        change = pd.concat([update_df,orig_df]).drop_duplicates(keep=False) # Get rows where value changed

        if change.shape[0] == 1: # For the first time data is entered
            change = change[change.columns[change.iloc[0] != '']]
        else:
            change = change[change.columns[change.iloc[0] != change.iloc[1]]].iloc[[0]] # Determine column in which value changed

    column = change.columns[-1] # Get the column in which value changed, either price or position
    pos_diff = ''


    if column == 'Price':
        lookup_price = float(change[column])
        lookup_qty = chart_df.loc[lookup_price, 'Qty/Level']
        lookup_pos = chart_df.loc[lookup_price, 'Position']

    if column == 'Position':
        chart_df = chart_df.reset_index()
        chart_df = chart_df.set_index('Position')
        max_pos = chart_df.index.max()
        min_pos = chart_df.index.min()
        lookup_pos = int(change[column])

        # Case one, position entered is exactly in the chart
        if lookup_pos in chart_df.index:
            lookup_price = chart_df.loc[lookup_pos, 'Price']
            lookup_qty = chart_df.loc[lookup_pos, 'Qty/Level']
        # Case two, position is inbetween and it is a sell chart
        # or, position is greater than max position in buy chart
        elif (re.search('sell', logic_type, re.IGNORECASE) and (lookup_pos > min_pos)) or \
             (re.search('buy', logic_type, re.IGNORECASE) and (lookup_pos > max_pos)):
            tmp = chart_df[chart_df.index < lookup_pos]
            pos_diff = tmp.index[-1] - lookup_pos
            lookup_price, lookup_qty = tmp.loc[tmp.index[-1]]
        #Case three, position is inbetween and it is a buy chart
        # or, position is greater than max position in sell chart
        else:
            tmp = chart_df[chart_df.index > lookup_pos].iloc[0]
            pos_diff = tmp.name - lookup_pos
            lookup_price, lookup_qty = tmp

    if run_unwind: # Determine the labels for the lookup row
        rlabel = 'Unwinding'
    else:
        rlabel = 'Adding'

    lookup_label = ' '.join([rlabel, 'Lookup'])
    diff_label = ' '.join([rlabel, 'Diff'])

    data_df = pd.DataFrame(data).set_index('Chart') # Set values
    data_df.loc[lookup_label, 'Price'] = lookup_price
    data_df.loc[lookup_label, 'Qty/Level'] = lookup_qty
    data_df.loc[lookup_label, 'Position'] = lookup_pos
    data_df.loc[diff_label, 'Position'] = pos_diff

    return data_df

def unwind_tiers(unwind, prod, price, scalp, tick_size, tier_len, tier_qty, tier_per):
    # ....... hidden .......#
    return df

def init_main_table(relationships, prod, dur):

    # Create columns
    # Read in file that is generated from RP morning scripts
    df = pd.read_pickle(Path(blueprint.root_path, 'data', 'prod_mmyy.pkl'))
    df.index = df.index.str.upper()
    columns = df[df.index == ''.join([prod, dur]).upper()].values.tolist()[0]
    columns = list(filter(None, columns))

    columns_dict = [
        {'name' : [i, j], 'id' : ' '.join([i, j])}
        for i, j in itertools.product(columns, ['B', 'S'])
    ]
    columns_dict.insert(0, {'name' : ['', ''], 'id' : 'Future'})

    rows = []
    rows.extend(relationships)

    # Create tooltip data
    tooltip_columns = [x['id'] for x in columns_dict]
    tooltip = []
    for rel in rows:
        tooltip.append({i : ' '.join([rel, i]) for i in tooltip_columns})

    # Create rows
    df = pd.DataFrame(columns=tooltip_columns)
    df['id'] = list(map(lambda x : ' '.join([prod.split("_")[0], x]), rows))
    df['Future'] = rows
    df = df.fillna('')

    query = IntraDayPositions.query.filter(IntraDayPositions.contract.contains(prod.split("_")[0]))
    pos_df = positions.get_positions(query = query)

    # Add positions to main table
    if not pos_df.empty:
        df = main_table_positions(df.set_index('id'), pos_df, prod)

    data = df.to_dict('records')

    style = []
    header_style = [{
        'if' : {'column_id' : 'Future'},
        'backgroundColor': 'rgb(230, 230, 230)',
        'fontWeight': 'bold'},
    ]

    style.extend(header_style)

    sell_style=[{
        'if': {'column_id': str(x), 'filter_query': '{{{0}}} < 0'.format(x)},
            'backgroundColor': 'rgb(250, 217, 222)',
        } for x in tooltip_columns
    ]

    style.extend(sell_style)

    buy_style=[{
        'if': {'column_id': str(x), 'filter_query': '{{{0}}} > 0'.format(x)},
            'backgroundColor': 'rgb(217, 230, 250)',
        } for x in tooltip_columns
    ]
    style.extend(buy_style)

    # Get risk report for current product and add to storage div
    if not pos_df.empty:
        risk_df = positions.get_risk_report(pos_df)
        risk_df.contract = risk_df.contract.str.lower().str.replace("_", " ")
    else:
        risk_df = pd.DataFrame()

    return risk_df, columns_dict, data, tooltip, style

def main_table_positions(row_df, pos_df, prod):
    #...... hidden .......#
    return row_df.reset_index()

def calculate_pos_risk(df, to_calculate='position'):
    if to_calculate == 'position':
        # ............. hidden ............. #
        return position
    else:
        # ............. hidden ............. #
        return risk

def get_add_unwind_type(string):
    if string == 'B':
        add = 'Buy'
        unwind = 'Sell Back'
    else:
        add = 'Sell'
        unwind = 'Buy Back'

    return add, unwind

def update_dropdown(product):
    if re.search('rbob', product, re.IGNORECASE):
        prod = 'Gasoline(rbob)'
    elif re.search('crack|brent', product, re.IGNORECASE): # Make sure crack is reading from the brent folder
        prod = 'Brent'
    else:
        prod = product

    file_path = Path(blueprint.root_path, 'data', prod.capitalize(), 'expired')
    files = os.listdir(file_path)

    if re.search('crack', product, re.IGNORECASE): # Make sure that crack doesn't include brent relationships
        files = [x for x in files if re.search('crack', x , re.IGNORECASE)]
    elif re.search('brent', product, re.IGNORECASE) and re.search('6', product, re.IGNORECASE): # Make sure that brent 6/12 doesn't include brent
        files = [x for x in files if (not re.search('crack', x , re.IGNORECASE)) and (re.search('6|12', x))]
    elif re.search('brent', product, re.IGNORECASE): # Make sure that brent doesn't include crack relationships
        files = [x for x in files if (not re.search('crack', x , re.IGNORECASE)) and (not re.search('6|12', x, re.IGNORECASE))]
    else:
        files = files

    files = [x.split(".")[0] for x in files]
    files = list(set(files))

    return files


#------------------------------------ TABLES HTML ------------------------------------------#
def main_table_html():

    table = dash_table.DataTable(
        id='main-table',
        merge_duplicate_headers = True,
        fixed_columns = {
            'headers' :True,
            'data': 1,
            },
        style_table = {
            'margin-top': '20px',
            'margin-bottom' : '20px',
            'minWidth' : '100%',
            },
        style_header = {
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'},
        style_cell = {
            'textAlign':'center',
            'minWidth' : '50px',
            #'width' : '100px',
            'font-family':'open sans'},
        css= [{
            'selector': 'td.cell--selected, td.focused',
            'rule': 'background-color: #fff4ba !important;'
            }, {
            'selector': 'td.cell--selected *, td.focused *',
            'rule': 'color: #6b7287 !important;'
            },{
            'selector': 'td.cell--selected *, td.focused *',
            'rule': 'font-weight: 600 !important;'

        }]
    )

    return table

def daily_rp_html(id):

    table = dash_table.DataTable(
        id = id,
        columns = [],
        data = [],
        style_header = {
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'},
        style_cell = {
            'textAlign':'center',
            'width' : '100px',
            'font-family':'open sans'},
        style_table = {
            'margin-top' : '5px'
        },
        #style_as_list_view=True,
    )

    return table

def baseline_heuristic_html(id):

    table = dash_table.DataTable(
        id = id,
        columns = [],
        data = [],
        style_header = {
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'},
        style_cell = {
            'textAlign':'center',
            'width' : '100px',
            'font-family':'open sans'},
        style_table = {
            'margin-top' : '20px'
        },
        style_data_conditional = [{
            'if' : {'column_id' : 'Params'},
            'textAlign' : 'left',
            'backgroundColor': 'rgb(230, 230, 230)',
            },
            {'if' : {'row_index' : 6},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 9},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 12},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 16},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 20},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 23},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 26},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 29},
            'backgroundColor': 'rgb(247, 247, 247)',
            }
        ],
        style_as_list_view=True,
    )

    return table

def tiers_table_html(id):
    table = dash_table.DataTable(
        id = id,
        style_header = {
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'},
        style_cell = {
            'textAlign':'center',
            'width' : '100px',
            'font-family':'open sans'},
        style_table = {
            'margin-top' : '20px'},
        style_data_conditional = [{
            'if' : {'column_id' : 'Chart'},
            'backgroundColor': 'rgb(230, 230, 230)',
            'textAlign' : 'left',
            },
            {'if' : {'row_index' : 4},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 7},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'row_index' : 10},
            'backgroundColor': 'rgb(247, 247, 247)',
            },
            {'if' : {'column_id' : 'Price', 'row_index' : 5},
            'border': '1px solid black',
            'backgroundColor' : '#FBE8CF',
            },
            {'if' : {'column_id' : 'Price', 'row_index' : 11},
            'border': '1px solid black',
            'backgroundColor' : '#FBE8CF',
            },
            {'if' : {'column_id' : 'Position', 'row_index' : 5},
            'border': '1px solid black',
            'backgroundColor' : '#FBE8CF',
            },
            {'if' : {'column_id' : 'Position', 'row_index' : 11},
            'border': '1px solid black',
            'backgroundColor' : '#FBE8CF',
            }
            ],
        style_as_list_view=True,
    )
    return table

def notes_html(id):
    textarea = dcc.Textarea(
        id = id,
        placeholder = 'Notes',
        title = 'Notes',
        value = '',
        style={'width' : '70%', 'min-height' : '400px', 'margin-top' : '60px'},
    )

    return textarea

def chart_html(id):
    table = dash_table.DataTable(
        id = id,
        columns = [],
        data = [],
        style_header = {
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'},
        style_cell = {
            'textAlign':'center',
            'width' : '100px',
            'font-family':'open sans'},
        style_table = {
            'margin-top' : '8px'
        },
        style_as_list_view=True,
    )
    return table

def generate_chart(trace, title):
    children = dcc.Graph(
        id='position-chart',
        figure={
            'data' : [trace],
            'layout' : {
                'title' : {'text' : title, 'font' : {'family' : "Open Sans", 'size' : 20, 'color' : '#6d7a91'} },
                'paper_bgcolor' : 'rgba(0,0,0,0)',
                'legend' : {'x' : 1.1, 'y' : 1.1},
                'xaxis' : {
                    'title' : {'text' : 'Days to Exp', 'font' : {'family' : "Open Sans", 'size' :18 , 'color' : '#6d7a91'} },
                    'anchor' : 'y',
                    'tickfont' : {'size' : 14, 'color' : '#6d7a91'},
                    #'tickvals' : trace['x'][::2], # Show every second tick
                    'autorange' : True,
                    'type' : 'category',
                    'zeroline' : False,
                    'linecolor' : '#6d7a91',
                    'linewidth' : 1,
                    'rangeslider' : {'visible' : True, 'thickness' : 0.01, 'borderwidth' : 1, 'bordercolor' : '#6d7a91'}
                },
                'yaxis' : {
                    'automargin' : True,
                    #'title' : {'text' : '$', 'font' : {'family' : "Open Sans", 'size' : 18, 'color' : '#6d7a91'} },
                    'zeroline' : False,
                    'anchor' : 'x',
                    "showgrid": False,
                    'linecolor' : '#6d7a91',
                    #'autorange' : 'reversed',
                    'tickfont' : {'size' : 14, 'color' : '#6d7a91'},
                    'linewidth' : 1,
                    'side' : 'left',
                }
            }
        }

    )

    return children
