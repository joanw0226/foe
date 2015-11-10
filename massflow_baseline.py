""" Mass Flow Baseline

This module contains functions to quantify
materials suitable for DRS from the conventional 
recycling and waste streams. The tonnages of 
dry recycling and waste data from each local 
authority are exported from WasteDataFlow,
for every quarter from January 2014 to march 2015.

Author: Joan Wang
"""

import numpy as np 
import pandas as pd 
import os.path as op  
import datetime as dt 
import time 

# The default data directory will be '../../data' 
# This can be redefined by the user of the function. 
data_dir = op.join('data') 
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def get_data():
    raw = pd.read_excel(op.join(data_dir, 'raw_jan14-sep15.xls'), 
                        sheetname='NotQ100', header= 1)
    raw = raw.drop(['CollateText','RowOrder','ColOrder','RowIdent',
                    'ColIdent','CollateID','columngroup'], axis=1)
    raw = raw[raw.Period != 'Jan 14 - Mar 14']
    return raw

"""
Household Kerbside Recycling

"""

def get_hhkerb_rec_qtr():
    raw = get_data()
    hhkerb_rec = raw[(raw.QuestionNumber == 'Q010') & 
                     (raw.ColText == 'Tonnage collected for recycling')]
    hhkerb_rec_qtr = hhkerb_rec.pivot_table(values='Data', index=['Authority','Period'],
                                         columns='RowText', aggfunc = lambda x: x).reset_index()
    return hhkerb_rec_qtr

def get_hhkerb_reu_la():
    raw = get_data()
    hhkerb_reu = raw[(raw.QuestionNumber == 'Q010') & 
                     (raw.ColText == 'Tonnage Collected for Reuse') & 
                     (raw.Data > 0)]
    #It has been verified that all the materials selected above can be added to sum_dry_rec
    hhkerb_reu_la = (hhkerb_reu.groupby('Authority')['Data'].agg(np.sum).to_frame().reset_index()
                         .rename(columns={'Data':'sum_dry_rec'}))
    return hhkerb_reu_la

def get_hhkerb_rec_la():
    hhkerb_rec_qtr = get_hhkerb_rec_qtr()
    hhkerb_rec_la = (hhkerb_rec_qtr.groupby('Authority').agg(np.sum).reset_index()
                     .drop(['Green garden waste only','Mixed garden and food waste',
                            'Waste food only'],axis=1))
    #Generate the sum of recycling materials (mostly just 'Co mingled materials' if applicable to LAs)
    hhkerb_rec_la['sum_dry_rec'] = hhkerb_rec_la.sum(axis=1)
    hhkerb_rec_la = hhkerb_rec_la[['Authority','Mixed glass','Mixed Plastic Bottles',
                                   'Plastics','Steel cans','Aluminium cans','Mixed cans',
                                   'Composite food and beverage cartons','Co mingled materials',
                                   'sum_dry_rec']]
    #Add hhkerb_reu_la to hhkerb_rec_la (only some LAs have data for hhkerb_reu_la)
    merge = hhkerb_rec_la.merge(get_hhkerb_reu_la(), how='left', on='Authority')
    merge['sum_dry_rec_y'] = merge['sum_dry_rec_y'].replace(np.NaN, 0)
    merge['sum_dry_rec'] = merge['sum_dry_rec_x'] + merge['sum_dry_rec_y']
    merge = merge.drop(['sum_dry_rec_x', 'sum_dry_rec_y'], axis=1)
    return merge

def get_hhkerb_rec_drs():
    hhkerb_rec_la = get_hhkerb_rec_la()
    hhkerb_rec_drs = hhkerb_rec_la
    
    #DRS Glass Bottles (derived from 'Mixed glass' or 'sum_dry_rec')
    #For those that have data for 'Mixed glass', WRAP rate is 66%
    hhkerb_rec_drs['DRS Glass Bottles'] = hhkerb_rec_drs['Mixed glass']*.66
    #Using co-mingled materials, WRAP rate is 15.99%
    hhkerb_rec_drs['DRS Glass Bottles'] = (hhkerb_rec_drs['DRS Glass Bottles']
                                            .replace(np.NaN, 
                                                     hhkerb_rec_drs['sum_dry_rec']*0.1599))
    
    #DRS Plastic Bottles (derived from 'Mixed Plastic Bottles', 'Plastics', or 'sum_dry_rec')
    #Mostly, 'Mixed Plastic Bottles' are treated as PET, HDPE, and Other. 
    #So DRS is assumed to be only PET and HDPE, 96%
    hhkerb_rec_drs['DRS Plastic Bottles'] = hhkerb_rec_drs['Mixed Plastic Bottles']*.96
    #If info not provided from 'Mixed Plastic Bottles', use 'Plastics'. 
    #For most of these LAs, plastics are dense plastics. Use WRAP rate of 68%
    hhkerb_rec_drs['DRS Plastic Bottles'] = np.where(hhkerb_rec_drs['DRS Plastic Bottles'].isnull(), 
                                                     hhkerb_rec_drs['Plastics']*.68, 
                                                     hhkerb_rec_drs['DRS Plastic Bottles'])
    #For Swansea, Plastics are dense plastics plus plastic film, so the rate is 57%
    hhkerb_rec_drs['DRS Plastic Bottles'] = np.where(hhkerb_rec_drs['Authority'] 
                                                     == 'City  and County of Swansea ',
                                                     hhkerb_rec_drs['Plastics']*.57, 
                                                     hhkerb_rec_drs['DRS Plastic Bottles'])
    #For LAs with co-mingled materials, WRAP rate is 6.69%
    hhkerb_rec_drs['DRS Plastic Bottles'] = (hhkerb_rec_drs['DRS Plastic Bottles']
                                             .replace(np.NaN,hhkerb_rec_drs['sum_dry_rec']*0.0669))
    
    #DRS Ferrous Cans & DRS Aluminium Cans
    #If 'Steel cans' or 'Aluminium cans' exists
    hhkerb_rec_drs['DRS Ferrous Cans'] = hhkerb_rec_drs['Steel cans']*.18
    hhkerb_rec_drs['DRS Aluminium Cans'] = hhkerb_rec_drs['Aluminium cans']*.80
    #Use 'Mixed cans' if available (WRAP rates: 27% is aluminium, 73% is ferrous)
    #For Neath Port Talbot and Powys, their data from Mixed Cans is incomplete. So skip those values.
    #DRS Ferrous Cans
    hhkerb_rec_drs['DRS Ferrous Cans'] = np.where((hhkerb_rec_drs['Mixed cans'].notnull()) & 
                                                  (hhkerb_rec_drs['Authority'] 
                                                  != 'Neath Port Talbot CBC') &
                                                  (hhkerb_rec_drs['Authority'] 
                                                  != 'Powys County Council'),
                                                  hhkerb_rec_drs['Mixed cans']*.73*.18,
                                                  hhkerb_rec_drs['DRS Ferrous Cans'])
    hhkerb_rec_drs['DRS Ferrous Cans'] = (hhkerb_rec_drs['DRS Ferrous Cans']
                                          .replace(np.NaN,hhkerb_rec_drs['sum_dry_rec']*0.0101))
    #DRS Aluminium Cans
    hhkerb_rec_drs['DRS Aluminium Cans'] = np.where((hhkerb_rec_drs['Mixed cans'].notnull()) &
                                                    (hhkerb_rec_drs['Authority'] 
                                                     != 'Neath Port Talbot CBC') &
                                                    (hhkerb_rec_drs['Authority'] 
                                                     != 'Powys County Council'),
                                                    hhkerb_rec_drs['Mixed cans']*.27*.8,
                                                    hhkerb_rec_drs['DRS Aluminium Cans'])
    hhkerb_rec_drs['DRS Aluminium Cans'] = (hhkerb_rec_drs['DRS Aluminium Cans']
                                            .replace(np.NaN,hhkerb_rec_drs['sum_dry_rec']*0.01688))

    #DRS Beverage Cartons
    #For two LAs with info on beverage cartons, use the specific data
    hhkerb_rec_drs['DRS Beverage Cartons'] = hhkerb_rec_drs['Composite food and beverage cartons']
    #For the rest, use WRAP rate of 0.31% on the sum of dry recycling
    hhkerb_rec_drs['DRS Beverage Cartons'] = (hhkerb_rec_drs['DRS Beverage Cartons']
                                               .replace(np.NaN, hhkerb_rec_drs['sum_dry_rec']*0.0031))
    hhkerb_rec_drs = hhkerb_rec_drs[['Authority','DRS Glass Bottles','DRS Plastic Bottles',
                                     'DRS Ferrous Cans','DRS Aluminium Cans','DRS Beverage Cartons']]
    
    #Export to .csv
    hhkerb_rec_drs.to_csv(op.join(data_dir,('hhkerb_rec_drs_'
                                            + dt.datetime.today().strftime("%d%m")
                                            + '.csv')), 
                          encodings = 'utf-8')
    return hhkerb_rec_drs

"""
Household Kerbside Residual Waste

"""

def get_hhkerb_res_qtr():
    raw = get_data()
    res = (raw[(raw.QuestionNumber == 'Q023') & (raw.ColText == 'Tonnage')]
             .pivot_table(values='Data', index=['Authority','Period'],
                          columns='RowText', aggfunc = lambda x: x)
             .reset_index())
    hhkerb_res_qtr = res[['Authority','Period','Collected household waste : Regular Collection']]
    return hhkerb_res_qtr

def get_hhkerb_rej_la():
    raw = get_data()
    hhkerb_rej_qtr = raw[(raw.QuestionNumber == 'Q010') & 
                     (raw.ColText == 'Tonnage collected for recycling but actually rejected/disposed')&
                     (raw.Data > 0)]
    hhkerb_rej_la = (hhkerb_rej_qtr.groupby('Authority').agg(np.sum).reset_index()
                     .drop(['Period','QuestionNumber','QuText','RowText',
                            'ColText','MaterialGroup'],axis=1))
    return hhkerb_rej_la

def get_hhkerb_res_la():
    hhkerb_res_qtr = get_hhkerb_res_qtr()
    hhkerb_res_la = hhkerb_res_qtr.groupby('Authority').agg(np.sum).reset_index()
    #Merge in rejected recycling (hhkerb_rej_la)
    merge = hhkerb_res_la.merge(get_hhkerb_rej_la(), how='left', on='Authority')
    merge['Data'] = merge['Data'].replace(np.NaN, 0)
    merge['Collected household waste : Regular Collection'] = (merge['Collected household waste : Regular Collection'] + merge['Data'])
    merge = merge.drop(['Data'],axis=1)
    return merge

def get_hhkerb_res_drs():
    hhkerb_res_la = get_hhkerb_res_la()
    hhkerb_res_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                             'DRS Plastic Bottles','DRS Ferrous Cans',
                                             'DRS Aluminium Cans','DRS Beverage Cartons'])
    hhkerb_res_drs['Authority'] = hhkerb_res_la['Authority']
    hhkerb_res_drs['DRS Glass Bottles'] = hhkerb_res_la['Collected household waste : Regular Collection']*0.0204
    hhkerb_res_drs['DRS Plastic Bottles'] = hhkerb_res_la['Collected household waste : Regular Collection']*0.0151
    hhkerb_res_drs['DRS Ferrous Cans'] = hhkerb_res_la['Collected household waste : Regular Collection']*0.001554
    hhkerb_res_drs['DRS Aluminium Cans'] = (hhkerb_res_la['Collected household waste : Regular Collection']*0.003255)
    hhkerb_res_drs['DRS Beverage Cartons'] = (hhkerb_res_la['Collected household waste : Regular Collection']*0.0037)
    hhkerb_res_drs.to_csv(op.join(data_dir, ('hhkerb_res_drs_'
                                               + dt.datetime.today().strftime("%d%m")
                                               + '.csv')), 
                            encodings = 'utf-8')
    return hhkerb_res_drs

"""
HWRCs Recycling

"""

def get_hwrcs_reu_la():
    raw = get_data()
    #Get reuse data from CA sites (Question 16)
    hwrcs_reu_ca = raw[(raw.QuestionNumber == 'Q016') & 
                     (raw.ColText == 'Tonnage collected for reuse') & 
                     (raw.Data > 0)]
    #It has been verified that all the materials selected above can be added to sum_dry_rec
    hwrcs_reu_ca_la = (hwrcs_reu_ca.groupby('Authority')['Data'].agg(np.sum).to_frame().reset_index()
                         .rename(columns={'Data':'sum_dry_rec'}))
    
    #Get reuse data from bring sites (Question 17)
    hwrcs_reu_bring = raw[(raw.QuestionNumber == 'Q017') & 
                     (raw.ColText == 'Tonnage collected for reuse') & 
                     (raw.Data > 0)]
    #It has been verified that all the materials selected above can be added to sum_dry_rec
    hwrcs_reu_bring_la = (hwrcs_reu_bring.groupby('Authority')['Data'].agg(np.sum).to_frame().reset_index()
                         .rename(columns={'Data':'sum_dry_rec'}))
    #Combine the two dataframes (add up reuse data for each LA)
    merge = hwrcs_reu_ca_la.merge(hwrcs_reu_bring_la, how='outer', on='Authority')
    merge = merge.replace(np.NaN, 0)
    merge['sum_dry_rec'] = merge['sum_dry_rec_x'] + merge['sum_dry_rec_y']
    merge = merge.drop(['sum_dry_rec_x','sum_dry_rec_y'],axis=1)
    return merge

def get_hwrcs_rec_la():
    drslist = ['Authority','Brown glass','Clear glass','Green glass','Mixed glass',
                                   'Mixed Plastic Bottles','Plastics',
                                   'Aluminium cans','Steel cans','Mixed cans',
                                   'Composite food and beverage cartons',
                                   'Co mingled materials','sum_dry_rec']
    raw = get_data()
    #Get recycling data from CA sites (Question 16)
    hwrcs_rec_ca = (raw[(raw.QuestionNumber == 'Q016') & 
                        (raw.ColText == 'Tonnage collected for recycling')]
                         .pivot_table(values='Data', index=['Authority','Period'],
                                      columns='RowText', aggfunc = lambda x: x).reset_index())
    hwrcs_rec_ca_la = (hwrcs_rec_ca.groupby('Authority').agg(np.sum).reset_index()
                     .drop(['Green garden waste only'],axis=1))
    #Get sum of dry recycling
    hwrcs_rec_ca_la['sum_dry_rec'] = hwrcs_rec_ca_la.sum(axis=1)
    #Keep DRS relevant columns, drop the rest
    hwrcs_rec_ca_la = hwrcs_rec_ca_la[drslist]    
    
    #Get recycling data from bring sites (Question 17)
    hwrcs_rec_bring = (raw[(raw.QuestionNumber == 'Q017') & 
                           (raw.ColText == 'Tonnage collected for recycling')]
                         .pivot_table(values='Data', index=['Authority','Period'],
                                      columns='RowText', aggfunc = lambda x: x).reset_index())
    hwrcs_rec_bring_la = (hwrcs_rec_bring.groupby('Authority').agg(np.sum).reset_index()
                          .drop(['Green garden waste only'],axis=1))
    #Get sum of dry recycling
    hwrcs_rec_bring_la['sum_dry_rec'] = hwrcs_rec_bring_la.sum(axis=1)
    #Keep DRS relevant columns, drop the rest
    hwrcs_rec_bring_la = hwrcs_rec_bring_la[drslist]
    #Add second to last row 'Vale of Glamorgan Council' and re-assign index
    hwrcs_rec_bring_la = (hwrcs_rec_bring_la.merge(hwrcs_rec_ca_la[['Authority']],
                                                   how='outer',on='Authority').sort('Authority'))
    hwrcs_rec_bring_la.index = range(0,len(hwrcs_rec_bring_la))
    #Before adding the two dataframes together, turn all missing values to 0
    hwrcs_rec_ca_la = hwrcs_rec_ca_la.replace(np.NaN,0)
    hwrcs_rec_bring_la = hwrcs_rec_bring_la.replace(np.NaN,0)
    #Add values of Question 16 and Question 17 together
    merge = (hwrcs_rec_ca_la + hwrcs_rec_bring_la)
    merge['Authority'] = hwrcs_rec_bring_la['Authority']
    
    #Merge in reuse data from get_hwrcs_reu_la()
    merge = merge.merge(get_hwrcs_reu_la(), how='left', on='Authority')
    merge = merge.replace(np.NaN, 0)
    merge['sum_dry_rec'] = merge['sum_dry_rec_x'] + merge['sum_dry_rec_y']
    merge = merge.drop(['sum_dry_rec_x','sum_dry_rec_y'],axis=1)
    merge.to_csv(op.join(data_dir, ('hwrcs_rec_la_'
                                    + dt.datetime.today().strftime("%d%m")+ '.csv')),
                 encodings = 'utf-8')
    return merge

def get_hwrcs_rec_drs():
    hwrcs_rec_la = get_hwrcs_rec_la()
    hwrcs_rec_drs = hwrcs_rec_la
    
    #DRS Glass Bottles (derived from 'Brown glass','Clear glass','Green glass',
    #'Mixed glass', or 'sum_dry_rec')
    #ZWS rate for 'Mixed glass' is 75%, 35% for 'Clear glass', and 100% for the rest
    hwrcs_rec_drs['DRS Glass Bottles'] = (hwrcs_rec_drs['Mixed glass']*.75 
                                          + hwrcs_rec_drs['Clear glass']*.35
                                          + hwrcs_rec_drs['Brown glass'] 
                                          + hwrcs_rec_drs['Green glass'])
    #Using co-mingled materials, WRAP MRF rate is ?????
    hwrcs_rec_drs['DRS Glass Bottles'] = (hwrcs_rec_drs['DRS Glass Bottles']
                                            .replace(0, 
                                                     hwrcs_rec_drs['sum_dry_rec']*0))
    
    #DRS Plastic Bottles (derived from 'Mixed Plastic Bottles', 'Plastics', or 'sum_dry_rec')
    #Use data from 'Mixed Plastic Bottles'. If unavailable, use data from 'Plastics'
    #'Mixed Plastic Bottles' are treated as 100% DRS, and ZWS rate for 'Plastics' is 50%
    #hwrcs_rec_drs['DRS Plastic Bottles'] = (hwrcs_rec_drs['Mixed Plastic Bottles'] 
    #                                        + hwrcs_rec_drs['Plastics']*.5)
    hwrcs_rec_drs['DRS Plastic Bottles'] = hwrcs_rec_drs['Mixed Plastic Bottles']
    hwrcs_rec_drs['DRS Plastic Bottles'] = (hwrcs_rec_drs['DRS Plastic Bottles']
                                            .replace(0,hwrcs_rec_drs['Plastics']*.5))
    
    #For LAs with co-mingled materials, WRAP rate is ???
    hwrcs_rec_drs['DRS Plastic Bottles'] = (hwrcs_rec_drs['DRS Plastic Bottles']
                                             .replace(0,hwrcs_rec_drs['sum_dry_rec']*0))
    
    #DRS Ferrous Cans & DRS Aluminium Cans
    #If 'Steel cans' or 'Aluminium cans' exists, use that info
    hwrcs_rec_drs['DRS Ferrous Cans'] = hwrcs_rec_drs['Steel cans']
    hwrcs_rec_drs['DRS Aluminium Cans'] = hwrcs_rec_drs['Aluminium cans']
    #Use 'Mixed cans' if 'Steel cans' or 'Aluminium cans' don't exist
    #ZWS rates: 20% is aluminium, 80% is ferrous
    hwrcs_rec_drs['DRS Ferrous Cans'] = (hwrcs_rec_drs['DRS Ferrous Cans']
                                         .replace(0,hwrcs_rec_drs['Mixed cans']*.8))
    #If none of the above exists, use sum of dry recycling, with WRAP MRF rate being ???
    hwrcs_rec_drs['DRS Ferrous Cans'] = (hwrcs_rec_drs['DRS Ferrous Cans']
                                         .replace(0,hwrcs_rec_drs['sum_dry_rec']*0))
    #Use 'Mixed cans' if 'Steel cans' or 'Aluminium cans' don't exist
    hwrcs_rec_drs['DRS Aluminium Cans'] = (hwrcs_rec_drs['DRS Aluminium Cans']
                                           .replace(0,hwrcs_rec_drs['Mixed cans']*.2))
    #If none of the above exists, use sum of dry recycling, with WRAP MRF rate being ???    
    hwrcs_rec_drs['DRS Aluminium Cans'] = (hwrcs_rec_drs['DRS Aluminium Cans']
                                            .replace(0,hwrcs_rec_drs['sum_dry_rec']*0))

    #DRS Beverage Cartons
    #For LAs with info on beverage cartons, use the specific data
    hwrcs_rec_drs['DRS Beverage Cartons'] = hwrcs_rec_drs['Composite food and beverage cartons']
    #For the rest, use WRAP MRS rate of ??? on the sum of dry recycling
    hwrcs_rec_drs['DRS Beverage Cartons'] = (hwrcs_rec_drs['DRS Beverage Cartons']
                                               .replace(0, hwrcs_rec_drs['sum_dry_rec']*0))
    hwrcs_rec_drs = hwrcs_rec_drs[['Authority','DRS Glass Bottles','DRS Plastic Bottles',
                                     'DRS Ferrous Cans','DRS Aluminium Cans','DRS Beverage Cartons']]
    
    #Export to .csv
    hwrcs_rec_drs.to_csv(op.join(data_dir,('hwrcs_rec_drs_'
                                            + dt.datetime.today().strftime("%d%m")
                                            + '.csv')), 
                          encodings = 'utf-8')
    return hwrcs_rec_drs

"""
HWRCs Residual Waste

"""

def get_hwrcs_res_qtr():
    raw = get_data()
    res = (raw[(raw.QuestionNumber == 'Q023') & (raw.ColText == 'Tonnage')]
             .pivot_table(values='Data', index=['Authority','Period'],
                          columns='RowText', aggfunc = lambda x: x)
             .reset_index())
    hwrcs_res_qtr = res[['Authority','Period','Civic amenity sites waste : Household']]
    return hwrcs_res_qtr

def get_hwrcs_rej_la():
    raw = get_data()
    #The only data of rejected materials is Tonnage collected for recycling but actually rejected / disposed
    #for Question 16
    hwrcs_rej_ca_qtr = raw[(raw.QuestionNumber == 'Q016') & 
                     (raw.ColText == 'Tonnage collected for recycling but actually rejected / disposed')&
                     (raw.Data > 0)]
    hwrcs_rej_la = (hwrcs_rej_ca_qtr.groupby('Authority').agg(np.sum).reset_index()
                     .drop(['Period','QuestionNumber','QuText','RowText',
                            'ColText','MaterialGroup'],axis=1))
    return hwrcs_rej_la

def get_hwrcs_res_la():
    hwrcs_res_qtr = get_hwrcs_res_qtr()
    hwrcs_res_la = hwrcs_res_qtr.groupby('Authority').agg(np.sum).reset_index()
    #Merge in rejected recycling (hwrcs_rej_la)
    merge = hwrcs_res_la.merge(get_hwrcs_rej_la(), how='left', on='Authority')
    merge['Data'] = merge['Data'].replace(np.NaN, 0)
    merge['Civic amenity sites waste : Household'] = (merge['Civic amenity sites waste : Household'] + merge['Data'])
    merge = merge.drop(['Data'],axis=1)    
    return merge

def get_hwrcs_res_drs():
    hwrcs_res_la = get_hwrcs_res_la()
    hwrcs_res_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                             'DRS Plastic Bottles','DRS Ferrous Cans',
                                             'DRS Aluminium Cans','DRS Beverage Cartons'])
    hwrcs_res_drs['Authority'] = hwrcs_res_la['Authority']
    hwrcs_res_drs['DRS Glass Bottles'] = hwrcs_res_la['Civic amenity sites waste : Household']*0.011665
    hwrcs_res_drs['DRS Plastic Bottles'] = hwrcs_res_la['Civic amenity sites waste : Household']*0.0066
    hwrcs_res_drs['DRS Ferrous Cans'] = hwrcs_res_la['Civic amenity sites waste : Household']*0.001824
    hwrcs_res_drs['DRS Aluminium Cans'] = hwrcs_res_la['Civic amenity sites waste : Household']*0.00248
    hwrcs_res_drs['DRS Beverage Cartons'] = hwrcs_res_la['Civic amenity sites waste : Household']*0.0007
    hwrcs_res_drs.to_csv(op.join(data_dir, ('hwrcs_res_drs_'
                                               + dt.datetime.today().strftime("%d%m")
                                               + '.csv')), 
                            encodings = 'utf-8')
    return hwrcs_res_drs

"""
Commerical Residual

"""

def get_com_res_qtr():
    raw = get_data()
    res = (raw[(raw.QuestionNumber == 'Q023') & (raw.ColText == 'Tonnage')]
             .pivot_table(values='Data', index=['Authority','Period'],
                          columns='RowText', aggfunc = lambda x: x)
             .reset_index())
    com_res_qtr = res[['Authority','Period','Collected non-household waste : Commercial & Industrial']]
    return com_res_qtr



"""
Mass flow baseline master function

"""
def get_massflow_baseline():
    hhkerb_rec_sum = (get_hhkerb_rec_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                          .rename(columns={0: 'Household Kerbside Recycling'}))
    hhkerb_res_sum = (get_hhkerb_res_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                            .rename(columns={0: 'Household Kerbside Residual'}))
    hwrcs_rec_sum = (get_hwrcs_rec_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                            .rename(columns={0: 'HWRCs Recycling'}))
    hwrcs_res_sum = (get_hwrcs_res_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                            .rename(columns={0: 'HWRCs Residual'}))
    
    #Merge everything in
    baseline = (hhkerb_rec_sum.merge(hhkerb_res_sum, right_index=True, left_index=True)
                .reset_index().rename(columns={'RowText': 'Authority'}))
    baseline = baseline.merge(hwrcs_rec_sum, left_on= 'Authority', right_index=True)
    baseline = baseline.merge(hwrcs_res_sum, left_on= 'Authority', right_index=True)
    
    #Export to .csv
    baseline.to_csv(op.join(data_dir, ('massflow_baseline_' 
                                       + dt.datetime.today().strftime("%d%m")
                                       + '.csv')), 
                    encodings = 'utf-8')
    return baseline

                            

    
                                                     

