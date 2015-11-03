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
from datetime import date 
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

def get_hhkerb_rec_ton_qtr(df=None):
    if df is None:
        df = get_data()
    hhkerb_rec = df[df.QuestionNumber == 'Q010']
    hhkerb_rec_ton = hhkerb_rec.groupby('ColText').get_group('Tonnage collected for recycling')
    hhkerb_rec_ton_qtr = hhkerb_rec_ton.pivot_table(values='Data', index=['Authority','Period'],
                                         columns='RowText', aggfunc = lambda x: x)
    hhkerb_rec_ton_qtr = hhkerb_rec_ton_qtr.reset_index()
    return hhkerb_rec_ton_qtr

def get_hhkerb_reu_ton_la(df=None):
    if df is None:
        df = get_data()
    hhkerb_rec = df[df.QuestionNumber == 'Q010']
    hhkerb_reu_ton = hhkerb_rec.groupby('ColText').get_group('Tonnage Collected for Reuse')
    hhkerb_reu_ton_qtr = hhkerb_reu_ton.pivot_table(values='Data', index=['Authority','Period'],
                                         columns='RowText', aggfunc = lambda x: x)
    hhkerb_reu_ton_qtr = hhkerb_reu_ton_qtr.reset_index()
    hhkerb_reu_ton_all = hhkerb_reu_ton_qtr.groupby('Authority').agg(np.sum)
    hhkerb_reu_ton_all = hhkerb_reu_ton_all.reset_index()
    hhkerb_reu_ton_all['sum_dry_rec'] = hhkerb_reu_ton_all.sum(axis=1)
    hhkerb_reu_ton_la = hhkerb_reu_ton_all[['Authority','sum_dry_rec']]
    return hhkerb_reu_ton_la

def get_hhkerb_rec_ton_la(df=None):
    if df is None:
        df = get_hhkerb_rec_ton_qtr()
    hhkerb_rec_ton_all = df.groupby('Authority').agg(np.sum)
    hhkerb_rec_ton_all = hhkerb_rec_ton_all.reset_index()
    hhkerb_rec_ton_all = hhkerb_rec_ton_all.drop(['Green garden waste only','Mixed garden and food waste',
                                              'Waste food only'],axis=1)
    #Generate the sum of recycling materials (for most LAs with co-mingled recycling, it is just from 'Co mingled materials')
    hhkerb_rec_ton_all['sum_dry_rec'] = hhkerb_rec_ton_all.sum(axis=1)
    hhkerb_rec_ton_agg = hhkerb_rec_ton_all.drop(['Absorbent Hygiene Products (AHP)','Car tyres',
                                                'Card','Gas bottles','Large vehicle tyres',
                                                'Mixed paper &  card','Other Scrap metal',
                                                'Other compostable waste','Other materials',
                                                'PS [6]','Paper',
                                                'Post consumer, non automotive batteries','Textiles & footwear',
                                                'Textiles only','Video tapes, DVDs and CDs',
                                                'WEEE - Fridges & Freezers','WEEE - Large Domestic App',
                                                'WEEE - Small Domestic App','Wood'],axis = 1)
    merge = hhkerb_rec_ton_agg.merge(get_hhkerb_reu_ton_la(), how='left', on='Authority')
    merge['sum_dry_rec_y'] = merge['sum_dry_rec_y'].replace(np.NaN, 0)
    merge['sum_dry_rec'] = merge['sum_dry_rec_x'] + merge['sum_dry_rec_y']
    merge = merge.drop(['sum_dry_rec_x', 'sum_dry_rec_y'], axis=1)
    return merge

def get_hhkerb_rec_ton_drs(df=None):
    if df is None:
        df = get_hhkerb_rec_ton_la()
    hhkerb_rec_ton_drs = df
    #DRS Glass Bottles
    #For those that have data for 'Mixed glass', WRAP rate is 66%
    hhkerb_rec_ton_drs['DRS Glass Bottles'] = hhkerb_rec_ton_drs['Mixed glass']*.66
    #Using co-mingled materials, WRAP rate is 15.99%
    hhkerb_rec_ton_drs['DRS Glass Bottles'] = (hhkerb_rec_ton_drs['DRS Glass Bottles']
                                            .replace(np.NaN, 
                                                     hhkerb_rec_ton_drs['sum_dry_rec']
                                                     *0.1599))
    #DRS Plastic Bottles
    #Mostly, 'Mixed Plastic Bottles' are treated as PET, HDPE, and Other. So DRS is assumed to be only PET and HDPE, 96%
    hhkerb_rec_ton_drs['DRS Plastic Bottles'] = hhkerb_rec_ton_drs['Mixed Plastic Bottles']*.96
    #If info not provided from 'Mixed Plastic Bottles', use 'Plastics'. 
    #For most of these LAs, plastics are dense plastics. Use WRAP rate of 68%
    hhkerb_rec_ton_drs['DRS Plastic Bottles'] = np.where(hhkerb_rec_ton_drs['DRS Plastic Bottles'].isnull(), 
                                                     hhkerb_rec_ton_drs['Plastics']*.68, 
                                                     hhkerb_rec_ton_drs['DRS Plastic Bottles'])
    #For Swansea, Plastics are dense plastics plus plastic film, so the rate is 57% (from dividing 6.69 from 11.66)
    hhkerb_rec_ton_drs[hhkerb_rec_ton_drs['Authority'] == 'City  and County of Swansea ']
    hhkerb_rec_ton_drs['DRS Plastic Bottles'] = np.where(hhkerb_rec_ton_drs['Authority'] == 'City  and County of Swansea ',
                                                     hhkerb_rec_ton_drs['Plastics']*.57, 
                                                     hhkerb_rec_ton_drs['DRS Plastic Bottles'])
    #For LAs with co-mingled materials, WRAP rate is 6.69%
    hhkerb_rec_ton_drs['DRS Plastic Bottles'] = (hhkerb_rec_ton_drs['DRS Plastic Bottles']
                                                 .replace(np.NaN,hhkerb_rec_ton_drs['sum_dry_rec']*0.0669))
    
    #DRS Ferrous Cans & DRS Aluminium Cans
    #If 'Steel cans' or 'Aluminium cans' exists
    hhkerb_rec_ton_drs['DRS Ferrous Cans'] = hhkerb_rec_ton_drs['Steel cans']*.18
    hhkerb_rec_ton_drs['DRS Aluminium Cans'] = hhkerb_rec_ton_drs['Aluminium cans']*.80
    #Use 'Mixed cans' if available (WRAP rates: 27% is aluminium, 73% is ferrous)
    #For Neath Port Talbot and Powys, their data from Mixed Cans is incomplete. So skip those values.
    #DRS Ferrous Cans
    hhkerb_rec_ton_drs['DRS Ferrous Cans'] = np.where((hhkerb_rec_ton_drs['Mixed cans'].notnull()) & 
                                                      (hhkerb_rec_ton_drs['Authority'] != 'Neath Port Talbot CBC') & 
                                                      (hhkerb_rec_ton_drs['Authority'] != 'Powys County Council'), 
                                                       hhkerb_rec_ton_drs['Mixed cans']*.73*.18, 
                                                       hhkerb_rec_ton_drs['DRS Ferrous Cans'])
    hhkerb_rec_ton_drs['DRS Ferrous Cans'] = (hhkerb_rec_ton_drs['DRS Ferrous Cans']
                                              .replace(np.NaN,hhkerb_rec_ton_drs['sum_dry_rec']*0.0101))
    #DRS Aluminium Cans
    hhkerb_rec_ton_drs['DRS Aluminium Cans'] = np.where((hhkerb_rec_ton_drs['Mixed cans'].notnull()) & 
                                                        (hhkerb_rec_ton_drs['Authority'] != 'Neath Port Talbot CBC') &
                                                        (hhkerb_rec_ton_drs['Authority'] != 'Powys County Council'), 
                                                        hhkerb_rec_ton_drs['Mixed cans']*.27*.8, 
                                                        hhkerb_rec_ton_drs['DRS Aluminium Cans'])
    hhkerb_rec_ton_drs['DRS Aluminium Cans'] = (hhkerb_rec_ton_drs['DRS Aluminium Cans']
                                                .replace(np.NaN,hhkerb_rec_ton_drs['sum_dry_rec']*0.01688))

    #DRS Beverage Cartons
    #For two LAs, use the specific data
    hhkerb_rec_ton_drs['DRS Beverage Cartons'] = hhkerb_rec_ton_drs['Composite food and beverage cartons']
    #For the rest, use WRAP rate of 0.31% on the sum of dry recycling
    hhkerb_rec_ton_drs['DRS Beverage Cartons'] = (hhkerb_rec_ton_drs['DRS Beverage Cartons']
                                                  .replace(np.NaN, hhkerb_rec_ton_drs['sum_dry_rec']*0.0031))
    hhkerb_rec_ton_drs.drop(['Aluminium cans','Co mingled materials','Composite food and beverage cartons','HDPE [2]',
                         'Mixed Plastic Bottles','Mixed cans','Mixed glass','Plastics','Steel cans','sum_dry_rec'],
                       axis =1, inplace = True)
    
    #Export to .csv
    hhkerb_rec_ton_drs.to_csv(op.join(data_dir, 'hhkerb_rec_ton_drs.csv'), encodings = 'utf-8')
    return hhkerb_rec_ton_drs

"""
Household Kerbside Waste

"""

def get_waste_ton_qtr(df=None):
    if df is None:
        df = get_data()
    waste_ton = df[(df.QuestionNumber == 'Q023') & (df.ColText == 'Tonnage')]
    waste_ton_wide = waste_ton.pivot_table(values='Data', index=['Authority','Period'],
                                           columns='RowText', aggfunc = lambda x: x)
    waste_ton_wide = waste_ton_wide.reset_index()
    return waste_ton_wide

def get_hhkerb_rej_ton_la(df=None):
    if df is None:
        df = get_data()
    hhkerb_rec = df[df.QuestionNumber == 'Q010']
    hhkerb_rej_ton = hhkerb_rec[(hhkerb_rec.ColText == 'Tonnage collected for recycling but actually rejected/disposed') 
               & (hhkerb_rec.Data > 0)]
    hhkerb_rej_ton_la = hhkerb_rej_ton.groupby('Authority').agg(np.sum)
    hhkerb_rej_ton_la = hhkerb_rej_ton_la.reset_index()
    hhkerb_rej_ton_la = hhkerb_rej_ton_la.drop(['Period','QuestionNumber','QuText','RowText',
                                                        'ColText','MaterialGroup'],axis=1)
    return hhkerb_rej_ton_la

def get_hhkerb_waste_ton_la(df=None):
    if df is None:
        df = get_waste_ton_qtr()
    hhkerb_waste_ton = df[['Authority','Period','Collected household waste : Regular Collection']]
    hhkerb_waste_ton_agg = hhkerb_waste_ton.groupby('Authority').agg(np.sum)
    hhkerb_waste_ton_agg = hhkerb_waste_ton_agg.reset_index()
    merge = hhkerb_waste_ton_agg.merge(get_hhkerb_rej_ton_la(), how='left', on='Authority')
    merge['Data'] = merge['Data'].replace(np.NaN, 0)
    merge['Collected household waste : Regular Collection'] = (merge['Collected household waste : Regular Collection'] 
                                                               + merge['Data'])
    merge.drop(['Data'],axis=1)
    return merge

def get_hhkerb_waste_ton_drs(df=None):
    if df is None:
        df = get_hhkerb_waste_ton_la()
    hhkerb_waste_ton_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                             'DRS Plastic Bottles','DRS Ferrous Cans',
                                             'DRS Aluminium Cans','DRS Beverage Cartons'])
    hhkerb_waste_ton_drs['Authority'] = df['Authority']
    hhkerb_waste_ton_drs['DRS Glass Bottles'] = df['Collected household waste : Regular Collection']*0.0204
    hhkerb_waste_ton_drs['DRS Plastic Bottles'] = df['Collected household waste : Regular Collection']*0.0151
    hhkerb_waste_ton_drs['DRS Ferrous Cans'] = df['Collected household waste : Regular Collection']*0.001554
    hhkerb_waste_ton_drs['DRS Aluminium Cans'] = (df['Collected household waste : Regular Collection']
                                                  *0.003255)
    hhkerb_waste_ton_drs['DRS Beverage Cartons'] = (df['Collected household waste : Regular Collection']
                                                    *0.0037)
    hhkerb_waste_ton_drs.to_csv(op.join(data_dir, 'hhkerb_waste_ton_drs.csv'), encodings = 'utf-8')
    return hhkerb_waste_ton_drs

"""
Mass flow baseline master function

"""
def get_massflow_baseline():
    hhkerb_rec_ton_sum = (get_hhkerb_rec_ton_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                          .rename(columns={0: 'Household Kerbside Recycling'}))
    hhkerb_waste_ton_sum = (get_hhkerb_waste_ton_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                            .rename(columns={0: 'Household Kerbside Residual'}))
    baseline = (hhkerb_rec_ton_sum.merge(hhkerb_waste_ton_sum, right_index=True, left_index=True)
                .reset_index().rename(columns={'RowText': 'Authority'}))
    baseline.to_csv(op.join(data_dir, 'massflow_baseline.csv'), encodings = 'utf-8')
    return baseline

                            

    
                                                     

