#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Celery process to download daily open interest from CME website and upload to the database

@author: sbhargava
"""

from .. import db, celery
from ..base.models import OpenInterest, Outright_OI
import requests
import pandas as pd
import datetime as dt
from time import sleep

# Define the CME products to look for and their URL ids, relationships for trading
p_dict = {
            'HO' : {'id' : 426, 'rel' : ['1m Fly', '2m Fly', '1m 2x', '2m 2x']},
            'LIVECATTLE' : {'id' : 22, 'rel' : ['2m Fly', '2m 2x']},
            'FEEDERCATTLE' : {'id' : 34, 'rel' : ['consecutive Fly', 'consecutive 2x']},
            'LEANHOGS' : {'id' : 19, 'rel' : ['consecutive Fly', 'consecutive 2x']},
            'WHEAT' : {'id' : 323, 'rel' : ['consecutive Fly', 'consecutive 2x']},
            'CORN' : {'id' : 300, 'rel' : ['consecutive Fly', 'consecutive 2x']},
            'KCWHEAT' : {'id' : 348, 'rel' : ['consecutive Fly', 'consecutive 2x']},
            'SOYBEANOIL' : {'id' : 312, 'rel' : ['consecutive Fly', 'consecutive 2x']},
            'LIGHTSWEETCRUDEOIL' : {'id' : 425, 'rel' : ['1m 2x', '2m 2x', '3m 2x']},
            'GASOLINE(RBOB)' : {'id' : 429, 'rel' : ['1m Fly', '1m 2x']},
            'NATURALGAS' : {'id' : 444, 'rel' : ['1m 2x', '2m 2x']},
        }


# Base url for downloading open interest data from CME website
# Change DATE, PORF and PRODUCTID
# ex. https://www.cmegroup.com/CmeWS/exp/voiTotalsViewExport.ctl?media=xls&tradeDate=20200218&reportType=F&productId=HO
base_url = 'https://www.cmegroup.com/CmeWS/exp/voiProductDetailsViewExport.ctl?media=xls&tradeDate=DATE&reportType=PORF&productId=PRODUCTID'

def request_data(url):
    # Pulls in csv file and returns as a dataframe
    r = requests.get(url)
    output = open('temp_output.xls', 'wb')
    output.write(r.content)
    output.close()

    df = pd.read_excel('temp_output.xls', nrows = 100, header=5, thousands=',')

    return df

def get_relevant_file(date, value, p_or_f):
    # Pulls in prelimnary/final result for open interest data for given product
    url = base_url.replace('DATE', date.strftime('%Y%m%d')).replace('PORF', p_or_f).replace('PRODUCTID', str(value))
    df = request_data(url)
    if df['Month'].isnull().idxmax() == 0: # returns an empty dataframe if the file is incomplete/incorrect
        return pd.DataFrame(), True
    else:
        df = df.iloc[:df['Month'].isnull().idxmax() - 1]
        return df, False

def get_relationships_oi(df, rel_list, prod, date_yest):
    # Calculates the open interest per contract by getting individual outrights' open interest
    # Relevant contracts are chosen according to the relationship defined
    rows_list = []
    for rel in rel_list:
        # rel = rel_list[0]

        if rel == '1m Fly' or rel == 'consecutive Fly' or (rel == '2m Fly' and prod == 'LIVECATTLE'):
            for i in range(df.shape[0]-2):
                cntr = ' '.join([prod, rel, df.loc[i, 'Month']])
                x = df.loc[i:i+2, 'OpenInterest']
                rows_list.append({'contract' : cntr, 'oi_1' : x.iloc[0], 'oi_2' : x.iloc[1], 'oi_3' : x.iloc[2] })

        elif rel == '2m Fly':
            for i in range(df.shape[0]-4):
                cntr = ' '.join([prod, rel, df.loc[i, 'Month']])
                x = df.loc[i:i+4:2, 'OpenInterest']
                rows_list.append({'contract' : cntr, 'oi_1' : x.iloc[0], 'oi_2' : x.iloc[1], 'oi_3' : x.iloc[2] })

        elif rel == '1m 2x' or rel == 'consecutive 2x' or (rel == '2m 2x' and prod == 'LIVECATTLE'):
            for i in range(df.shape[0]-3):
                cntr = ' '.join([prod, rel, df.loc[i, 'Month']])
                x = df.loc[i:i+3, 'OpenInterest']
                rows_list.append({'contract' : cntr, 'oi_1' : x.iloc[0], 'oi_2' : x.iloc[1], 'oi_3' : x.iloc[2], 'oi_4' : x.iloc[3] })

        elif rel == '2m 2x':
            for i in range(df.shape[0]-6):
                cntr = ' '.join([prod, rel, df.loc[i, 'Month']])
                x = df.loc[i:i+6:2, 'OpenInterest']
                rows_list.append({'contract' : cntr, 'oi_1' : x.iloc[0], 'oi_2' : x.iloc[1], 'oi_3' : x.iloc[2], 'oi_4' : x.iloc[3] })
        else:
            print (rel + ' Relationship not defined')

    rel_df = pd.DataFrame(rows_list)
    rel_df['date'] = date_yest

    return rel_df

# Define periodic task that runs every morning at 7 am
@celery.task(bind=True, name='Open-Interest-to-DB')
def main(self):
    date_today = dt.date.today()
    date_yest = date_today - dt.timedelta(1)

    rel_df_list = []
    out_df_list = []
    for prod, value in p_dict.items():
        # prod = 'LEANHOGS'; value = {'id' : 19, 'rel' : ['consecutive Fly', 'consecutive 2x']}
        print (prod)

        flag = True
        # Getting the relevant file
        # 1) Try to get 'final' open interest data for most recent date (yesterday). If that fails
        #    get the 'prelimnary' data for the most recent date.
        # 2) If no data is available for yesterday, continue loop until the last available date.
        while flag:
            df, flag = get_relevant_file(date_yest, value['id'], 'F')

            if df.empty:
                df, flag = get_relevant_file(date_yest, value['id'], 'P')

            if flag:
                date_yest = date_yest - dt.timedelta(1)

        # Outright wise open interest
        df = df[['Month', 'At Close']]
        df.columns = ['Month', 'OpenInterest']
        df['Month'] = df['Month'].str.replace(' ', '').str.capitalize()
        df['Product'] = prod
        df['date'] = date_yest

        if prod == 'LEANHOGS':
        # traders don't trade May outright
            df = df[~df.Month.str.contains('May')].reset_index(drop=True)

        # Contract wise open interest
        rel_df = get_relationships_oi(df, value['rel'], prod, date_yest)
        rel_df_list.append(rel_df)
        out_df_list.append(df)

        sleep(2) # To prevent multiple, fast requests to the CME website

    rel_df = pd.concat(rel_df_list, ignore_index=True)
    rel_df = rel_df.astype(object).where(pd.notnull(rel_df), None)


    ret_vals = []
    try:
        # Add contract-wise data to the database using SQLAlchemy
        db.session.bulk_insert_mappings(OpenInterest, rel_df.to_dict('records'))
        db.session.commit()
        ret_vals.append('SUCESS: contract OI')
    except:
        db.session.rollback()
        ret_vals.append('ERROR: contract OI')

    out_df = pd.concat(out_df_list, ignore_index=True)

    try:
        # Add outright data to the database using SQLAlchemy
        db.session.bulk_insert_mappings(Outright_OI, out_df.to_dict('records'))
        db.session.commit()
        ret_vals.append('SUCESS: outright OI')
    except:
        db.session.rollback()
        ret_vals.append('ERROR: outright OI')

    return ret_vals

