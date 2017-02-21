#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 18:29:03 2017

@author: felixstoeckmann
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
    
    columns = ['biomass_ahead','biomass_actual','lignite_ahead','lignite_actual',
               'deriv_coal_ahead','deriv_coal_actual','gas_ahead','gas_actual',
               'coal_ahead','coal_actual','oil_ahead','oil_actual',
               'shell_oil_ahead','shell_oil_actual','peat_ahead','peat_actual',
               'geotherm_ahead','geotherm_actual','hps_ahead','hps_actual',
               'ror_ahead','ror_actual','hydro_res_ahead','hydro_res_actual',
               'marine_ahead','marine_actual','nuklear_ahead','nuklear_actual',
               'other_ahead','other_actual','other_ee_ahead','other_ee_actual',
               'solar_ahead','solar_actual','wast_ahead','wast_actual',
               'wind_os_ahead','wind_os_actual','wind_ahead','wind_actual']
               
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
    gen_df = pd.DataFrame(index = index,columns = ['day_ahead','actual'])
    for h in gen_df.index:
        for c in gen_df.columns:
            gen_df.at[h,c] = crawler_df.loc[dt.datetime.strftime(h,'%Y%m%d %H'),c].sum()/4

    return gen_df
