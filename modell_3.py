# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 13:01:30 2015

@author: felix
"""
import numpy as np
import pandas as pd
import datetime as dt
import pytz
import os
import modell_slave_3 as mod_s
import entsoe_data as entd

path = os.path.dirname(os.path.realpath(__file__))

Emis   = {'Erdgas':0.202,'Steinkohle':0.339,'Braunkohle':0.404,'Minaraloel':0.280, 
          'Import':0.4718}

#year, month, day = 2015, 4, 8
#year, month, day = 2013, 07, 10
#date = dt.datetime(year,month,day, tzinfo = pytz.timezone('Europe/Berlin'))


###############################################################################
'''
Alle Funktionen mit 'Get' Lesenexterne Datein ein. Zum Teil werden in diesen
Fuktionen noch weitere Dateispezifische Prozesse vollzogen.
'''
###############################################################################


def load_func(Master,kon):
    
    try:# Erster Versuch: Original Last Daten
        if all(pd.notnull(Master['load_actual'])):
            Load = Master['load_actual']
        elif any(pd.notnull(Master['load_actual'])):
            raise TypeError
        elif all(pd.isnull(Master['load_actual'])):
            raise TypeError
    except TypeError:
        if all(pd.isnull(Master['load_actual'])):
        # Dritter Versuch: Gesamte Ahead Last Daten
            if all(pd.notnull(Master['load_day_ahead'])):
                Load = Master['load_day_ahead']
            elif all(pd.isnull(Master['load_day_ahead'])):
                for l in Master.index:# Letzte Möglichkeit Auffsumation de EEX Energieträger 
                    if pd.isnull(Master.at[l,'Load']):
                        Master.at[l,'load_day_ahead'] = Master.ix[l][kon].sum()
                Load = Master['load_actual']
 
        if any(pd.isnull(Master['load_actual'])):
            # Zweiter Versuch: Stundenweise Ahead Last Daten oder Interpolieren
            LineNr = find(pd.isnull(Master['load_actual']))
            for q in LineNr:
                if pd.isnull(Master.Load.ix[q]):
                    if pd.isnull(Master['load_day_ahead'].ix[q]) or Master['load_day_ahead'].ix[q] == 'N/A':
                        Master.Load = Master['Load'].astype(float).interpolate(method = 'linear')
                    else:
                        Master.Load.ix[q] = float(Master['load_day_ahead'].ix[q])

            Load = Master['load_actual']

    for hour in Load.index:
        try:
            if (np.abs(Load[hour] - Load[hour-1]) > 15000) or (np.abs(Load[hour] - Load[hour+1]) > 15000):
                if pd.isnull(Master.at[hour,'load_day_ahead']) == False:
                    Load[hour] = float(Master.at[hour,'load_day_ahead'])
                else:
                    if np.abs(Load[hour] - Load[hour-1]) > 15000:
                        Load[hour] = Load[hour-1]
                    elif np.abs(Load[hour] - Load[hour+1]) > 15000:
                        Load[hour] = Load[hour+1]

                    print ('its not allright at:', hour)
        except KeyError:
            pass

    Load = Load.astype(float)
    return (Load)



def verteilung(MO_Year,AvYear,date):
    '''Kernstück des Modells. Übertragung der Merit-Order(Jahresweise), 
    Jahresverfügbarkeiten und dem jeweils zu berechnenden Datums.
    Im erste Schritt wird der Master und die tagesbezogene MO eingeladen.\n
    Dependence: Get_Master(), MO_daily() und wird aufgerufen von Store()
    '''
    Master = entd.master_file(date2)
    Master = mod_s.get_master(date)
    MO_Av  = mod_s.get_active_plants(MO_Year,AvYear,date)
    
    #ModLa = np.round(Master['Load']*(1/0.86)-Master[Sub].sum(axis=1)+Master[Add].sum(axis=1),decimals=1)
    kon = ['nuklear','coal','deriv_coal','gas','lignite','oil','shell_oil','other','peat',
           'pumped_storage','run_of_river','hydro_reservoir','solar','wind']
           
    Wasser = ['pumped_storage','run_of_river','hydro_reservoir']
    ColSe =  ['Last*','Import','Export','Biomasse','Wasser','Wind','Solar','Kon-Last']
    
    Bio = mod_s.biogas(Master.index, date)

    '''Im dritten Schritt werden die Gesamtlastdaten über die Funktion 
    load_func gerpüft und notfalls aufgefüllt.'''
    
    Load = load_func(Master, kon)*(1/0.86)

    '''Im vierten Schritt wird der DataFrame zur Berechnung erstellt und die 
    Daten aus de Master eingefügt. Wobei sich die konventionelle Last wie folgt
    zusammen setzt: 
    L_kon = L_entsoe * 1/0,86 - EE - Import - Wasser + Export'''
    
    Sub = ['import','wind','solar','pumped_storage','run_of_river','hydro_reservoir']
    Add = ['export']    
    
    ModSeg = pd.concat([Load,Master['import'],Master['export'],Bio[0],
                        Master[Wasser].sum(axis=1),Master['wind'],Master['solar'],
                        Load-Master[Sub].sum(axis=1)-Bio[0]+Master[Add].sum(axis=1)
                       ],axis = 1)
    
    ModSeg.columns = ColSe
    
    Add = pd.DataFrame()
    Add_Col = ['Kernenergie','Braunkohle','Steinkohle','Gas','Oel','Import*','Export*','CO2-Faktor',
               'CO2-Absolut']
    index = ModSeg.index
    for hour in ModSeg.index:
        KK = BK = SK = Ga = Oi = Im = Ex = CO2 = 0
        HourLoad = ModSeg.at[hour,'Kon-Last']
        for P in MO_Av.index:
            if MO_Av.at[P,'Grenzkosten'] == 0:
                if MO_Av.at[P,'Energietraeger'] == 'Braunkohle':
                    BK = BK + MO_Av.at[P,'Leistung']
                    CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Braunkohle']/MO_Av.at[P,'Wirkungsgrad'])
                elif MO_Av.at[P,'Energietraeger'] == 'Steinkohle':
                    SK = SK + MO_Av.at[P,'Leistung']
                    CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Steinkohle']/MO_Av.at[P,'Wirkungsgrad'])
                elif MO_Av.at[P,'Energietraeger'] == 'Erdgas':
                    Ga = Ga + MO_Av.at[P,'Leistung']
                    CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Erdgas']/MO_Av.at[P,'Wirkungsgrad'])
                elif MO_Av.at[P,'Energietraeger'] == 'Mineralölprodukte':
                    Oi = Oi + MO_Av.at[P,'Leistung']
                    CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Minaraloel']/MO_Av.at[P,'Wirkungsgrad'])
            
            elif MO_Av.at[P,'Grenzkosten'] != 0:
                if [KK + BK + SK + Ga + Oi] < HourLoad:
                    if MO_Av.at[P,'Energietraeger'] == 'Kernenergie':
                        KK = KK + MO_Av.at[P,'Leistung']
                    elif MO_Av.at[P,'Energietraeger'] == 'Braunkohle':
                        BK = BK + MO_Av.at[P,'Leistung']
                        CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Braunkohle']/MO_Av.at[P,'Wirkungsgrad'])                        
                    elif MO_Av.at[P,'Energietraeger'] == 'Steinkohle':
                        SK = SK + MO_Av.at[P,'Leistung']
                        CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Steinkohle']/MO_Av.at[P,'Wirkungsgrad'])
                    elif MO_Av.at[P,'Energietraeger'] == 'Erdgas':
                        Ga = Ga + MO_Av.at[P,'Leistung']
                        CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Erdgas']/MO_Av.at[P,'Wirkungsgrad'])
                    elif MO_Av.at[P,'Energietraeger'] == 'Mineralölprodukte':
                        Oi = Oi + MO_Av.at[P,'Leistung']
                        CO2 = CO2 + MO_Av.at[P,'Leistung'] * (Emis['Minaraloel']/MO_Av.at[P,'Wirkungsgrad'])
            
            if [KK + BK + SK + Ga + Oi] >= HourLoad:
                COF = CO2/ModSeg.at[hour,'Last*']
                Ex = KK + BK + SK + Ga + Oi - HourLoad 
                PreAdd = pd.DataFrame(data = [[KK,BK,SK,Ga,Oi,Im,Ex,COF,CO2]],columns = Add_Col, index = None)
                Add = Add.append(PreAdd)
                break
            elif P == MO_Av.last_valid_index():
                Im = HourLoad - KK - BK - SK - Ga - Oi

                COIm = Emis['Import']*Master.at[hour,'Import']
                CO2 = CO2 + COIm
                COF = CO2/ModSeg.at[hour,'Last*']
                PreAdd = pd.DataFrame(data = [[KK,BK,SK,Ga,Oi,Im,Ex,COF,CO2]],columns = Add_Col, index = None)
                Add = Add.append(PreAdd)
                break
        
    Add = Add.set_index(index)
    Mod_Master = pd.concat([ModSeg,Add],axis = 1)
    Roundup = ['Last*','Import','Export','Biomasse','Wasser','Wind','Solar',
               'Kon-Last','Braunkohle','CO2-Absolut','Gas','Import*','Export*',
               'Kernenergie','Oel','Steinkohle']
    Mod_RoundI = np.round(Mod_Master[Roundup].astype(float),decimals=1)
    Mod_RoundII = np.round(Add['CO2-Faktor'].astype(float),decimals=3)
    Mod_RoundII = pd.DataFrame(Mod_RoundII)
    Mod_Master = pd.concat([Mod_RoundI,Mod_RoundII],axis = 1)
    #Mod_Master = [Mod_Master[Roundup].apply(np.round,decimals = 1),
    #               Mod_Master['CO2-Faktor'].apply(np.round,decimals = 3)]
    return Mod_Master


def store_func(year):
    '''Aktiviert die einzelen Fuktionen und speichert die Daten der Berechnung
    jahreweise ab. Ist somit auch leicht abänderbar in eine tägliche 
    Berechnungsstruktur.\n
    Dependence: Get_Available(), MeritOrder()
    '''
    AvYear  = mod_s.get_available(year)
    MO_Year = mod_s.merit_order(year)
    
    month, day = 1, 1
    
    Day = dt.datetime(year, month, day)
    Endyear = dt.datetime(year+1, month, day)
    
    Datpath = path + '/Daten/Modell Daten/Test XXV/' + str(year)

    try:
        os.makedirs(Datpath)
    except OSError:
        pass
    try:
        while Day < Endyear:
            month, day = Day.month, Day.day
            fileName = Datpath + '/' + dt.datetime.strftime(Day,'%Y%m%d')+'-Modell_Master.csv'
    
            if os.path.isfile(fileName) == True:
                print (dt.datetime.strftime(Day, '%Y %m %d'),'existing.')
                pass
            else:
                data = verteilung(MO_Year,AvYear,Day)
                data.to_csv(fileName, sep=',', header = True)
                print (dt.datetime.strftime(Day, '%Y %m %d'),'done.')
                
            Day += dt.timedelta(days=1)
    except KeyboardInterrupt:
        print ('Keyboard interrupted exit')

def store():
    for o in [2013,2014,2015]:
        store_func(o)
