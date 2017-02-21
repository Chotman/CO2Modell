# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 15:16:23 2016

@author: felix
"""

import numpy as np
import pandas as pd
import datetime as dt
import os

path = os.path.dirname(os.path.realpath(__file__))
ZP = 5 #Zertifikatskosten

Emis   = {'Erdgas':0.202,'Steinkohle':0.339,'Braunkohle':0.404,'Minaraloel':0.28, 
          'Import':0.4718}

#year, month, day = 2015, 4, 8
#year, month, day = 2013, 07, 10
#date = dt.datetime(year,month,day, tzinfo = pytz.timezone('Europe/Berlin'))


###############################################################################
'''
Alle Funktionen mit 'Get' Lesenexterne Datein ein. Zumteil werden in diesen
Fuktionen noch weitere Dateispezifische Prozesse vollzogen.
'''
###############################################################################

def get_available(year):
    '''Liest die Liste der Verfügbaren Kraftwerksleistungen nach Energieträger
    ein. Die Liste liegt als CSV-Datei vor. Und muss so an einem Ort zur 
    verfügung gestellt werden, das programm kümmersich nicht selbständig um die
    Beschaffung.\n
    Dependence: Keine, muss aufgerufen werden. Gilt als Eingang von Verteilung()
    '''
    AvPath = path + '/Daten/Kraftwerke/Available/'
    AvCap = os.listdir(AvPath)
    for y in AvCap:
        if y[:4] == str(year):
            fname = AvPath + y
    
    data = pd.read_csv(fname, sep = ';', skiprows = 3)
    data = data[data['[Country]']== 'DE']

    startdate = dt.datetime(year, 1, 1)
    enddate = startdate + dt.timedelta(days = 364)
    index = pd.date_range(startdate,enddate, freq = 'D')#,tz = 'Europe/Berlin')

    EType = ['coal','gas','lignite','oil','uranium']    
    AvYear = pd.DataFrame(columns = EType, index = index)
    for i in EType:
        data_i = data[data['[Source]']== i]
        for g in data_i.index:
            try:
                date = dt.datetime.strptime(data.at[g,'[TimeStamp]'][:10],'%Y-%m-%d')
                AvYear.at[date,i] = data.at[g,'[AvailableCapacity]']# and data.at[d,'[Source]'] == i: 
            except TypeError:
                pass
    return AvYear


def get_master(date):
    '''Die Funktion list einen gewünschte MasterMix CSV-Datei. Diese wurde 
    bereits, in einem anderen Programmteil zusaen gestellt und auf dem Laufwerk
    abgelegt.\n
    Dependence: Keine, wird aufgerufen von Verteilung()
    '''
    year = date.year

    fname  = path +'/Daten/MasterMix/'+str(year) + '/' + dt.datetime.strftime(date,'%Y%m%d')+'-Master.csv'
    
    Master = pd.read_csv(fname, sep = ',', index_col = [0])
    startdate =  dt.datetime.strptime(Master.index[0][:19],'%Y-%m-%d %H:%M:%S')
    enddate =  dt.datetime.strptime(Master.index[-1][:19],'%Y-%m-%d %H:%M:%S')
    index = pd.date_range(startdate,enddate, freq = 'H',tz = 'Europe/Berlin')
    Master = Master.set_index(index)

    return Master


def get_yearly_active_plants(year):
    '''Die Funktion liest die Matser-Kraftwerksliste der aktiven Kraftwerke ein.
    Dependence: Keine, wird aufgerufen von Verteilung()
    '''    

    Plants = pd.read_csv(path + '/Daten/Kraftwerke/Kranftwerksliste_Master.csv')

    for i in range(0,len(Plants)):
        Plants.at[i ,'Baujahr'] = float(Plants.at[i,'Baujahr'])
        Plants.at[i,'Leistung'] = float(Plants.at[i,'Leistung'])
        if Plants.at[i,'Voraus_Stillegung'] == '--':
            Plants.at[i,'Voraus_Stillegung'] = 2100
        else:
            Plants.at[i,'Voraus_Stillegung'] = float(Plants.at[i,'Voraus_Stillegung'])
            
    

    NewP_I = Plants[(Plants['Betriebstand'] == 'In Bau') | (Plants['Betriebstand'] == 'In Planung') | (Plants['Betriebstand'] == 'in Betrieb')]
    NewP = NewP_I[(NewP_I.Voraus_Stillegung >= year)]
    NewP = NewP[NewP.Baujahr <= int(year)]

    index = np.arange(len(NewP))
    NewP = NewP.set_index(index)
    
    return NewP


def get_active_plants(MO_Year,AvYear,date):
    '''Die Funktion liest die modifizierte Master-Kraftwerksliste ein und 
    beschneidet sie nach Jahr und Betriebszustand.
    Dependence: MO_daily, wird aufgerufen von MeritOrder()
    '''
    MO_Av = mo_daily(MO_Year,AvYear,date)
    
    try:
        NewDate = date + dt.timedelta(days = -1)
        AktivName = path + '/Daten/Kraftwerke/Temp/'+ dt.datetime.strftime(NewDate,'%Y%m%d')+'_Aktiv_Plants.csv'
        Aktive = pd.read_csv(AktivName)
        MO_Av['Aktiv'] = 'N'
        
        for K in MO_Av.index:
            BNA_Nr = MO_Av.at[K,'BNetzA_ID']
            if [Aktive[Aktive['BNetzA_ID']==BNA_Nr].Aktiv == 'KWK']:
                MO_Av.at[K,'Aktiv'] = 'KWK'
            elif [Aktive[Aktive['BNetzA_ID']==BNA_Nr].Aktiv == 'A']:
                MO_Av.at[K,'Aktiv'] = 'A'
            else:
                MO_Av.at[K,'Aktiv'] = 'N'
        
        #os.remove(AktivName)
        
    except IOError:
        MO_Av['Aktiv'] = 'N'
        MO_Av['Aktiv'][MO_Av['Energietraeger'] == 'Kernenergie'] = 'A'
    
    return MO_Av


def grenzkosten(Plant):
    '''Die Funktion berechnet die Grenzkosten eines Kraftwerks. Die Berechnung
    stütz sich auf den Energieträger und die Größe des jeweiligen Kraftwerkes,
    nur bei Gaskraftweken kommt es zu einer weiteren Unterscheidung
    (GuD:Ja/Nein). Die Funktionen zur Berechnung deWirkungsgrade und der 
    Betriebskosten(Vk) sind der BA-Stöckmann entnommen.\n
    Dependence: Keine, wird aufgerufen von MeritOrder()
    '''
    def gk_func(Et,Ar,LG):
        Emissionen  = {'Erdgas':0.202, 'Steinkohle':0.339, 
                       'Braunkohle':0.404, 'Minaraloel':5}

        if Et == 'Erdgas':
            if Ar == 'GuD':
                Wg = 0.000126*LG+0.47614
            else:
                Wg = 0.0274*np.log(LG)+0.223
            Vk = 0.0000018*LG**2-0.0061*LG + 9.1
            EF = Emissionen['Erdgas']
            Bk = 29
        
        elif Et == 'Steinkohle':
            if LG > 800:
                Wg = 0.0000001768*(LG**2)-0.00010586*LG+0.3715707
            elif LG <=800:
                Wg = 0.00011713*LG+ 0.30629575
            Vk = 0.000002*LG**2-0.009*LG+17
            EF = Emissionen['Steinkohle']
            Bk = 15        

        elif Et == 'Braunkohle':
            Wg = 0.0304*np.log(LG)+0.18
            Vk = 0.000002*LG**2-0.01*LG+19.5
            EF = Emissionen['Braunkohle']
            Bk = 3.8          

        elif Et == 'Kernenergie':
            Wg = 1
            Vk = 0.000002*LG**2-0.01*LG+19.5
            EF = 0
            Bk = 1

        elif Et == 'Mineralölprodukte':
            Wg = 0.35
            Vk = 0.0000018*LG**2-0.0061*LG + 9.1# Wie Erdgas
            EF = Emissionen['Minaraloel']
            Bk = 28
        
        Gk = Bk/Wg + ZP * EF/Wg + Vk
    
        return (Gk,Wg)
    
    Et = Plant['Energietraeger']
    Ar = Plant['Kraftwerksart']
    LG = Plant['Leistung']
    
    GK,Wg = gk_func(Et,Ar,LG)
    
    return (GK,Wg)


def merit_order(year):
    '''Erstellt aus der Kraftwerksliste die Merit-Order und wählt die Kraftweke 
    zum KWK-Betrieb aus die mit Vorrang in das Netz speisen.\n
    Dependence: Get_YearlyActivePlants(), Grenzkosten()
    '''
    NewP = get_yearly_active_plants(year)
    
    col  = list(NewP.columns) + list(['Grenzkosten','Wirkungsgrad'])
    MO_Year = pd.DataFrame(columns = col)
    
    for i in NewP.index:
        Plant = NewP.iloc[i]
        try:
            GK = float("%.2f"%grenzkosten(Plant)[0])
            Wg = float("%.4f"%grenzkosten(Plant)[1])
            GkP = np.append(Plant.values, [GK, Wg])
            d = pd.DataFrame(data = [GkP],columns = col,index = None)
            MO_Year  = MO_Year.append([d])
        except UnboundLocalError:
            pass

    MO_Year = MO_Year.sort_values(by = 'Grenzkosten')
    index = np.arange(len(MO_Year))
    MO_Year = MO_Year.set_index(index)

    return MO_Year


def mo_daily(MO_Year,AvYear,date):
    '''Erstellt eine täglich neu berechnete MO mit sich ändernden KWK- und \n 
    Gesamtverfügbarkeiten. Wobei Gas und Steinkohle künstlich erhöht werden \n
    Dependence: Keine, wird aufgerufen von Verteilung()
    '''
    MO    = MO_Year.copy()
    DATE  = dt.datetime.strftime(date,'%Y-%m-%d')
    AvDay = AvYear.loc[DATE]
    '''Berrechnung der KWK-Leistungen je Energieträger'''
    
    KWK_L = {'Braunkohle':553.6,'Steinkohle':1660.9,'Erdgas':6073.1,
             'Minraloel':296.8}
    
    f_gas   = 1+0.1*np.cos(np.pi/182*dt.date.timetuple(date).tm_yday+np.pi/183)
    f_sk    = 1+0.2*np.cos(np.pi/182*dt.date.timetuple(date).tm_yday+np.pi/183)
    KWK_Gas = KWK_BrK = KWK_StK = KWK_Oel= pd.DataFrame(columns = MO.columns)
    
    MO_KWK = pd.concat([MO[MO['KWK'] == 'ja'],MO[MO['KWK'] == 'Ja']])
    MO_KWK = MO_KWK.sort_values(by = 'Leistung',ascending = False)
    MO_KWK = MO_KWK.set_index(np.arange(len(MO_KWK)))
    
    for p in MO_KWK.index:
        if MO_KWK.at[p,'Energietraeger'] == 'Erdgas':
            if KWK_Gas.Leistung.sum() + MO_KWK.at[p,'Leistung'] > f_gas*KWK_L['Erdgas']:
                pass
            elif KWK_Gas.Leistung.sum() + MO_KWK.at[p,'Leistung'] < f_gas*KWK_L['Erdgas']:
                ID = MO_KWK.at[p,'BNetzA_ID']
                i  = MO[MO['BNetzA_ID']==ID].index
                MO['Grenzkosten'][i] = 0
                d = pd.DataFrame(data = [MO_KWK.iloc[p]],index = None)
                KWK_Gas = KWK_Gas.append(d)
            else: pass

        elif MO_KWK.at[p,'Energietraeger'] == 'Braunkohle':
            if KWK_BrK.Leistung.sum() + MO_KWK.at[p,'Leistung'] > KWK_L['Braunkohle']:
                pass
            elif KWK_BrK.Leistung.sum() + MO_KWK.at[p,'Leistung'] < KWK_L['Braunkohle']:
                ID = MO_KWK.at[p,'BNetzA_ID']
                i  = MO[MO['BNetzA_ID']==ID].index
                MO['Grenzkosten'][i] = 0
                d = pd.DataFrame(data = [MO_KWK.iloc[p]],index = None)
                KWK_BrK = KWK_BrK.append(d)
            else: pass

        elif MO_KWK.at[p,'Energietraeger'] == 'Steinkohle':
            if KWK_StK.Leistung.sum() + MO_KWK.at[p,'Leistung'] > f_sk*KWK_L['Steinkohle']:
                pass
            elif KWK_StK.Leistung.sum() + MO_KWK.at[p,'Leistung'] < f_sk*KWK_L['Steinkohle']:
                ID = MO_KWK.at[p,'BNetzA_ID']
                i  = MO[MO['BNetzA_ID']==ID].index
                MO['Grenzkosten'][i] = 0
                d = pd.DataFrame(data = [MO_KWK.iloc[p]],index = None)
                KWK_StK = KWK_StK.append(d)
            else: pass

        elif MO_KWK.at[p,'Energietraeger'] == 'Mineralölprodukte':
            if KWK_Oel.Leistung.sum() + MO_KWK.at[p,'Leistung'] > KWK_L['Minraloel']:
                pass
            elif KWK_Oel.Leistung.sum() + MO_KWK.at[p,'Leistung'] < KWK_L['Minraloel']:
                ID = MO_KWK.at[p,'BNetzA_ID']
                i  = MO[MO['BNetzA_ID']==ID].index
                MO['Grenzkosten'][i] = 0
                d = pd.DataFrame(data = [MO_KWK.iloc[p]],index = None)
                KWK_Oel = KWK_Oel.append(d)
            else: pass
    
    MO = MO.sort_values(by = 'Grenzkosten')
    MO = MO.set_index(np.arange(len(MO)))
    
    '''Berrechnungen der Verfügbarkeiten je Energieträger'''
    MO_KK = MO[MO['Energietraeger'] == 'Kernenergie']
    MO_BK = MO[MO['Energietraeger'] == 'Braunkohle']
    MO_SK = MO[MO['Energietraeger'] == 'Steinkohle']
    MO_Ga = MO[MO['Energietraeger'] == 'Erdgas']
    MO_Oi = MO[MO['Energietraeger'] == 'Mineralölprodukte']
    
    MO_KK = MO_KK.set_index(np.arange(len(MO_KK)))
    MO_BK = MO_BK.set_index(np.arange(len(MO_BK)))
    MO_SK = MO_SK.set_index(np.arange(len(MO_SK)))
    MO_Ga = MO_Ga.set_index(np.arange(len(MO_Ga)))
    MO_Oi = MO_Oi.set_index(np.arange(len(MO_Oi)))

    '''Korrektur der Verfügbarkeiten!!'''
    KK_max,BK_max = AvDay['uranium'],AvDay['lignite'],
    SK_max,Ga_max,Oi_max = AvDay['coal']*1.37,AvDay['gas']*1.1,AvDay['oil']*1.2
    
    KK_Av = BK_Av = SK_Av = Ga_Av = Oi_Av = 0
    KK = BK = SK = Ga = Oi = pd.DataFrame()
    for K in MO_KK.index:
        if [KK_Av + MO_KK.at[K,'Leistung']] < KK_max:
            KK_Av = KK_Av + MO_KK.at[K,'Leistung']
            KAdd = pd.DataFrame(data = [MO_KK.iloc[K]],index = None)
            KK = KK.append(KAdd)
        elif [KK_Av + MO_KK.at[K,'Leistung']] > KK_max:
            MO_KK.at[K,'Leistung'] = KK_max - KK_Av
            KAdd = pd.DataFrame(data = [MO_KK.iloc[K]],index = None)
            KK = KK.append(KAdd)
            break
    
    for K in MO_BK.index:
        if [BK_Av + MO_BK.at[K,'Leistung']] < BK_max:
            BK_Av = BK_Av + MO_BK.at[K,'Leistung']
            KAdd = pd.DataFrame(data = [MO_BK.iloc[K]],index = None)
            BK = BK.append(KAdd)
        elif [BK_Av + MO_BK.at[K,'Leistung']] > BK_max:
            MO_BK.at[K,'Leistung'] = BK_max - BK_Av
            KAdd = pd.DataFrame(data = [MO_BK.iloc[K]],index = None)
            BK = BK.append(KAdd)
            break

    for K in MO_SK.index:
        if [SK_Av + MO_SK.at[K,'Leistung']] < SK_max:
            SK_Av = SK_Av + MO_SK.at[K,'Leistung']
            KAdd = pd.DataFrame(data = [MO_SK.iloc[K]],index = None)
            SK = SK.append(KAdd)
        elif [SK_Av + MO_SK.at[K,'Leistung']] > SK_max:
            MO_SK.at[K,'Leistung'] = SK_max - SK_Av
            KAdd = pd.DataFrame(data = [MO_SK.iloc[K]],index = None)
            SK = SK.append(KAdd)
            break
        
    for K in MO_Ga.index:
        if [Ga_Av + MO_Ga.at[K,'Leistung']] < Ga_max:
            Ga_Av = Ga_Av + MO_Ga.at[K,'Leistung']
            KAdd = pd.DataFrame(data = [MO_Ga.iloc[K]],index = None)
            Ga = Ga.append(KAdd)
        elif [Ga_Av + MO_Ga.at[K,'Leistung']] > Ga_max:
            MO_Ga.at[K,'Leistung'] = Ga_max - Ga_Av
            KAdd = pd.DataFrame(data = [MO_Ga.iloc[K]],index = None)
            Ga = Ga.append(KAdd)
            break

    for K in MO_Oi.index:
        if [Oi_Av + MO_Oi.at[K,'Leistung']] < Oi_max:
            Oi_Av = Oi_Av + MO_Oi.at[K,'Leistung']
            KAdd = pd.DataFrame(data = [MO_Oi.iloc[K]],index = None)
            Oi = Oi.append(KAdd)
        elif [Oi_Av + MO_Oi.at[K,'Leistung']] > Oi_max:
            MO_Oi.at[K,'Leistung'] = Oi_max - Oi_Av
            KAdd = pd.DataFrame(data = [MO_Oi.iloc[K]],index = None)
            Oi = Oi.append(KAdd)
            break
    
    
    MO_Av = pd.concat([KK,BK,SK,Ga,Oi])    
    MO_Av = MO_Av.sort_values(by = 'Grenzkosten')
    MO_Av = MO_Av.set_index(np.arange(len(MO_Av)))
    
    return MO_Av


def biogas(index,date):
    '''Berechnet die Leistung der Biogasanlagen.
    Dependence: keine
    '''    
    if date.year == 2013:
        bio = date.month*21.758+4565.4
    elif date.year == 2014:
        bio = date.month*9.066+4851.3
    else:
        bio = date.month*9.066+(date.year*109-214667)
    
    bio = bio * 1+0.2*np.cos(np.pi/182*dt.date.timetuple(date).tm_yday+np.pi/183)
    
    Bio = pd.DataFrame(data=[bio]*len(index),index = index)
    
    return Bio