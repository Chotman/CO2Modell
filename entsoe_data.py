#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 18:29:03 2017

@author: felix
"""

import pandas as pd
import numpy as np
import bs4 as bs
import requests
import os.path
import datetime as dt
import pytz

tz = pytz.timezone('Europe/Berlin') 
year,month,day = 2017,2,20
date = tz.localize(dt.datetime(year,month,day))


def load_crwaler(date):
    DATE =  dt.datetime.strftime(date,'%d.%m.%Y')
    url = 'https://transparency.entsoe.eu/load-domain/r2/totalLoadR2/show?name=&defaultValue=false&viewType=TABLE&areaType=CTY&atch=false&dateTime.dateTime='+DATE+'+00:00|CET|DAY&biddingZone.values=CTY|10Y1001A1001A83F!CTY|10Y1001A1001A83F&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)'
    res = requests.get(url)
    html = res.text
    soup = bs.BeautifulSoup(html)
    result = soup.find_all('td' ,{'class': 'first','class':'dv-value-cell',
                                                   'class':'dv-value-cell'})
    crawler_index = index_day(year,month,day,'crawler')
    crawler_df = pd.DataFrame(index = crawler_index,columns = ['day_ahead','actual'])

    l = 0
    for i in range(0,len(result)):
        c = i%2
        try:
            crawler_df.iat[l,c] = int(result[i].text)
        except ValueError:
            crawler_df.iat[l,c] = np.nan
        if c == 1:
            l += 1
    
    index = index_day(year,month,day,'')
    load_df = pd.DataFrame(index = index,columns = ['day_ahead','actual'])
    for h in load_df.index:
        for c in load_df.columns:
            load_df.at[h,c] = crawler_df.loc[dt.datetime.strftime(h,'%Y%m%d %H'),c].sum()/4
    
    return load_df

def gen_crwaler(date):
    DATE =  dt.datetime.strftime(date,'%d.%m.%Y')
    url = 'https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/show?name=&defaultValue=false&viewType=TABLE&areaType=CTY&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime=20.02.2017+00:00|CET|DAYTIMERANGE&dateTime.endDateTime='+DATE+'+00:00|CET|DAYTIMERANGE&area.values=CTY|10Y1001A1001A83F!CTY|10Y1001A1001A83F&productionType.values=B01&productionType.values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType.values=B07&productionType.values=B08&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType.values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B20&productionType.values=B15&productionType.values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)'
    res = requests.get(url)
    html = res.text
    soup = bs.BeautifulSoup(html)
    result = soup.find_all('td' ,{'class':'first','class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell',
                                  'class':'dv-value-cell','class':'dv-value-cell'})
    
    columns = ['biomass_aggreg','biomass_consum','lignite_aggreg','lignite_consum',
               'deriv_coal_aggreg','deriv_coal_consum','gas_aggreg','gas_consum',
               'coal_aggreg','coal_consum','oil_aggreg','oil_consum',
               'shell_oil_aggreg','shell_oil_consum','peat_aggreg','peat_consum',
               'geotherm_aggreg','geotherm_consum','hps_aggreg','hps_consum',
               'ror_aggreg','ror_consum','hydro_res_aggreg','hydro_res_consum',
               'marine_aggreg','marine_consum','nuklear_aggreg','nuklear_consum',
               'other_aggreg','other_consum','other_ee_aggreg','other_ee_consum',
               'solar_aggreg','solar_consum','wast_aggreg','wast_consum',
               'wind_os_aggreg','wind_os_consum','wind_aggreg','wind_consum']
               
    crawler_index = index_day(year,month,day,'crawler')
    crawler_df = pd.DataFrame(index = crawler_index,columns = columns)
    l = 0
    for i in range(0,len(result)):
        c = i%40
        try:
            crawler_df.iat[l,c] = int(result[i].text)
        except ValueError:
            crawler_df.iat[l,c] = np.nan
        if c == 39:
            l += 1
    
    index = index_day(year,month,day,'')
    gen_df = pd.DataFrame(index = index,columns = columns)
    for h in gen_df.index:
        for c in gen_df.columns:
            gen_df.at[h,c] = crawler_df.loc[dt.datetime.strftime(h,'%Y%m%d %H'),c].sum()/4

    return gen_df

def imex_port_crawler(date):
    DATE =  dt.datetime.strftime(date,'%d.%m.%Y')
    imexport_index = index_day(year,month,day,'')
    imexport_df = pd.DataFrame(index =  imexport_index,columns = ['import','export'])
    crossing = ['10YAT-APG------L','10YCZ-CEPS-----N','10Y1001A1001A65H','10YFR-RTE------C', 
                '10YLU-CEGEDEL-NQ','10YNL----------L','10YPL-AREA-----S','10YSE-1--------K',
                '10YCH-SWISSGRIDZ']
    im, ex = [], []
    for cross in crossing:
        url = 'https://transparency.entsoe.eu/transmission-domain/physicalFlow/show?name=&defaultValue=false&viewType=TABLE&areaType=BORDER_CTY&atch=false&dateTime.dateTime='+DATE+'+00:00|CET|DAY&border.values=CTY|10Y1001A1001A83F!CTY_CTY|10Y1001A1001A83F_CTY_CTY|'+cross+'&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)'
        res = requests.get(url)
        html = res.text
        soup = bs.BeautifulSoup(html)
        result = soup.find_all('td' ,{'class': 'first','class':'dv-value-cell',
                                                       'class':'dv-value-cell'})
        crawler_index = index_day(year,month,day,'')
        crawler_df = pd.DataFrame(index = crawler_index,columns = ['import_'+cross[3:5],
                                                                   'export_'+cross[3:5]])
        im += ['import_'+cross[3:5]]
        ex += ['export_'+cross[3:5]]
        l = 0
        for i in range(0,len(result)):
            c = i%2
            try:
                crawler_df.iat[l,c] = int(result[i].text)
            except ValueError:
                crawler_df.iat[l,c] = np.nan
            if c == 1:
                l += 1
        
        imexport_df = pd.concat([imexport_df,crawler_df],axis = 1)
    
    imexport_df['import'],imexport_df['export'] = imexport_df[im].sum(axis=1),imexport_df[ex].sum(axis=1)
    
    return imexport_df




def index_day(year, month, day, typ):
    startdate = tz.localize(dt.datetime(year, month, day, hour=0, minute= 0))
    Datum  = range(25,32)
    for i in Datum:
        d = tz.localize(dt.datetime(year, 3, i,0,0))
        b = tz.localize(dt.datetime(year, 10, i,0,0))
        if d.weekday() == 6:
            ds = d
        if b.weekday() == 6:
            dh = b
    if startdate < ds:
        enddate = startdate + dt.timedelta(minutes = 59,hours = 23,)
    elif startdate == ds:
        enddate = startdate + dt.timedelta(minutes = 59,hours = 22)    
    elif startdate > ds and startdate < dh:
        startdate = startdate - dt.timedelta(hours = 1)
        enddate = startdate + dt.timedelta(minutes = 59,hours = 23)    
    elif startdate == dh:
        startdate = startdate - dt.timedelta(hours = 1)
        enddate = startdate + dt.timedelta(minutes = 59,hours = 24)
    if startdate > dh:
        enddate = startdate + dt.timedelta(minutes = 59,hours = 23)
    
    if typ == 'crawler':
        index = pd.date_range(startdate, enddate, freq = '15min')
    else:
        index = pd.date_range(startdate, enddate, freq = 'H')
    return (index)