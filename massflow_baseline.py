""" Mass Flow Baseline
Author: Joan Wang
Contact: joan20226@gmail.com
Affiliatoin: Friends of the Earth Cymru
Date: November 27th, 2015

This module contains functions to quantify
materials suitable for DRS from the conventional 
recycling and waste streams. The tonnages of 
dry recycling and waste data from each local 
authority over four quarters (April 2014 
to March 2015) are exported from WasteDataFlow.

This module includes functions to complete
the mass flow baseline of the information 
described above, and functions that develops
different mass flows based on different scenarios
of DRS return rates.

Most functions automatically output the resulting 
dataframe in a CSV file.

Other supplementary data are from the following:
The Office of National Statistics - 
http://www.ons.gov.uk/ons/publications/re-reference-tables.html?edition=tcm%3A77-368259
Eunomia -
http://www.eunomia.co.uk/reports-tools/a-scottish-deposit-refund-system/
WRAP -  
http://www.wrapcymru.org.uk/content/composition-municipal-solid-waste-wales-0 
Jemma Bere and colleagues at Keep Wales Tidy
"""

""" How to use this module:

The following section should be copied and executed in
an iPython notebook:

import numpy as np 
import pandas as pd 
import os.path as op  
import datetime as dt 
import time 
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
data_dir = op.join('data') 
import massflow_baseline

Note: The default data directory will be 'data', 
but this can be redefined by the user of this module. 
For example, if the raw data spreadsheet sits in the 
directory ./data/foe/drs/ (in relation to current directory) 
then the data_dir can be changed to 
data_dir = op.join('data','foe','drs')

Then, any function defined this module can be executed
in the iPython notebook.

For example, to execute the function that displays the
mass flow baseline and automatically saves it in an .csv file, 
execute the following in a command box:

massflow_baseline.get_massflow_baseline()

Note: The first "massflow_baseline" is the name of this module
The ".get_massflow_baseline()" is calling the function in the module

For more information on the functions, please read the comments attached.
"""

import numpy as np 
import pandas as pd 
import os.path as op  
import datetime as dt 
import time 
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
data_dir = op.join('data') 

""" 
Import raw data

"""

def get_data():
    """
    Input: Excel spreadsheet exported from WasteDataFlow
    Output: Raw data from WasteDataFlow from April 2014 to March 2015,
    excluding some irrelevant columns.
    """
    raw = pd.read_excel(op.join(data_dir, 'raw_jan14-sep15.xls'), 
                        sheetname='NotQ100', header= 1)
    raw = raw.drop(['CollateText','RowOrder','ColOrder','RowIdent',
                    'ColIdent','CollateID','columngroup'], axis=1)
    raw = raw[raw.Period != 'Jan 14 - Mar 14']
    return raw

def get_pop():
    """
    Input: Table from get_data()
    Output: Population for each local authority
    """
    raw = get_data()
    pop_qtr = raw[(raw.QuestionNumber == 'Q001') & (raw.RowText == 'Population of Authority')]
    pop_la = pop_qtr[['Authority','Data']].drop_duplicates().rename(columns={'Data':'Population'})
    pop_la = pop_la.sort('Authority').reset_index().drop('index', axis=1)
    return pop_la

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
    return hhkerb_rec_la

def get_hhkerb_recreu_la():
    raw = get_data()
    hhkerb_reu = raw[(raw.QuestionNumber == 'Q010') & 
                     (raw.ColText == 'Tonnage Collected for Reuse') & 
                     (raw.Data > 0)]
    #It has been verified that all the materials selected above can be added to sum_dry_rec
    hhkerb_reu_la = (hhkerb_reu.groupby('Authority')['Data'].agg(np.sum).to_frame().reset_index()
                         .rename(columns={'Data':'sum_dry_rec'}))

    #Add hhkerb_reu_la to hhkerb_rec_la (only some LAs have data for hhkerb_reu_la)
    hhkerb_rec_la = get_hhkerb_rec_la()
    merge = hhkerb_rec_la.merge(hhkerb_reu_la, how='left', on='Authority')
    merge['sum_dry_rec_y'] = merge['sum_dry_rec_y'].replace(np.NaN, 0)
    merge['sum_dry_rec'] = merge['sum_dry_rec_x'] + merge['sum_dry_rec_y']
    merge = merge.drop(['sum_dry_rec_x', 'sum_dry_rec_y'], axis=1)
    return merge

def get_hhkerb_rec_drs(reuse='No', method='WRAP', dry_rec = 'Sum', comingled_reject = 'Yes'):
    if reuse == 'No':
        hhkerb_rec_drs = get_hhkerb_rec_la()
    if reuse == 'Yes':
        hhkerb_rec_drs = get_hhkerb_recreu_la()
    
    if comingled_reject == 'Yes':
        reject_rate = 0.8915
    if comingled_reject == 'No':
        reject_rate = 1.0
    
    #These are WRAP rates
    mixed_glass = 0.6564
    co_glass = 0.1599
    bottles = 0.9626
    plastics = 0.6847
    swansea = 0.5738
    co_plastics = 0.0669
    steel = 0.1953
    alum = 0.9483
    mixed_fer = 0.1453 
    mixed_alum = 0.2429
    co_fer = 0.0101
    co_alum = 0.0169
    co_bev = 0.0031
    if method=='Eunomia':
        mixed_glass = 0.80
        plastics = 0.22
        
    if dry_rec == 'Sum':
        dry_rec = 'sum_dry_rec'
    if dry_rec == 'Comingled':
        dry_rec = 'Co mingled materials'
    
    #DRS Glass Bottles (derived from 'Mixed glass' or 'sum_dry_rec')
    #For those that have data for 'Mixed glass'
    hhkerb_rec_drs['DRS Glass Bottles'] = hhkerb_rec_drs['Mixed glass'] * mixed_glass
    #Using co-mingled materials
    hhkerb_rec_drs['DRS Glass Bottles'] = (hhkerb_rec_drs['DRS Glass Bottles']
                                            .replace(np.NaN, 
                                                     hhkerb_rec_drs[dry_rec] * reject_rate * co_glass))
    
    #DRS Plastic Bottles (derived from 'Mixed Plastic Bottles', 'Plastics', or 'sum_dry_rec')
    #Mostly, 'Mixed Plastic Bottles' are treated as PET, HDPE, and Other. 
    #So DRS is assumed to be only PET and HDPE
    hhkerb_rec_drs['DRS Plastic Bottles'] = hhkerb_rec_drs['Mixed Plastic Bottles'] * bottles
    #If info not provided from 'Mixed Plastic Bottles', use 'Plastics'. 
    #For most of these LAs, plastics are dense plastics.
    hhkerb_rec_drs['DRS Plastic Bottles'] = (hhkerb_rec_drs['DRS Plastic Bottles']
                                             .replace(np.NaN,hhkerb_rec_drs['Plastics'] * plastics))
    #For Swansea, Plastics are dense plastics plus plastic film, use WRAP adjusted rate
    hhkerb_rec_drs['DRS Plastic Bottles'] = np.where(hhkerb_rec_drs['Authority'] 
                                                     == 'City  and County of Swansea ',
                                                     hhkerb_rec_drs['Plastics'] * swansea, 
                                                     hhkerb_rec_drs['DRS Plastic Bottles'])
    #For LAs with co-mingled materials
    hhkerb_rec_drs['DRS Plastic Bottles'] = (hhkerb_rec_drs['DRS Plastic Bottles']
                                             .replace(np.NaN,
                                                      hhkerb_rec_drs[dry_rec]*reject_rate*co_plastics))
    
    #DRS Ferrous Cans & DRS Aluminium Cans
    #If 'Steel cans' or 'Aluminium cans' exists
    hhkerb_rec_drs['DRS Ferrous Cans'] = hhkerb_rec_drs['Steel cans'] * steel
    hhkerb_rec_drs['DRS Aluminium Cans'] = hhkerb_rec_drs['Aluminium cans'] * alum
    #Use 'Mixed cans' if available
    #For Neath Port Talbot and Powys, their data from Mixed Cans is incomplete. So skip those values.
    #DRS Ferrous Cans
    hhkerb_rec_drs['DRS Ferrous Cans'] = np.where((hhkerb_rec_drs['Mixed cans'].notnull()) & 
                                                  (hhkerb_rec_drs['Authority'] 
                                                  != 'Neath Port Talbot CBC') &
                                                  (hhkerb_rec_drs['Authority'] 
                                                  != 'Powys County Council'),
                                                  hhkerb_rec_drs['Mixed cans'] * mixed_fer,
                                                  hhkerb_rec_drs['DRS Ferrous Cans'])
    hhkerb_rec_drs['DRS Ferrous Cans'] = (hhkerb_rec_drs['DRS Ferrous Cans']
                                          .replace(np.NaN,hhkerb_rec_drs[dry_rec]*reject_rate*co_fer))
    #DRS Aluminium Cans
    hhkerb_rec_drs['DRS Aluminium Cans'] = np.where((hhkerb_rec_drs['Mixed cans'].notnull()) &
                                                    (hhkerb_rec_drs['Authority'] 
                                                     != 'Neath Port Talbot CBC') &
                                                    (hhkerb_rec_drs['Authority'] 
                                                     != 'Powys County Council'),
                                                    hhkerb_rec_drs['Mixed cans'] * mixed_alum,
                                                    hhkerb_rec_drs['DRS Aluminium Cans'])
    hhkerb_rec_drs['DRS Aluminium Cans'] = (hhkerb_rec_drs['DRS Aluminium Cans']
                                            .replace(np.NaN,
                                                     hhkerb_rec_drs[dry_rec]*reject_rate*co_alum))

    #DRS Beverage Cartons
    #For two LAs with info on beverage cartons, use the specific data
    hhkerb_rec_drs['DRS Beverage Cartons'] = hhkerb_rec_drs['Composite food and beverage cartons']
    #For the rest, use WRAP rate of 0.31% on the sum of dry recycling
    hhkerb_rec_drs['DRS Beverage Cartons'] = (hhkerb_rec_drs['DRS Beverage Cartons']
                                               .replace(np.NaN,
                                                        hhkerb_rec_drs[dry_rec]*reject_rate*co_bev))
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

def get_hhkerb_res_la():
    hhkerb_res_qtr = get_hhkerb_res_qtr()
    hhkerb_res_la = hhkerb_res_qtr.groupby('Authority').agg(np.sum).reset_index()
    return hhkerb_res_la

def get_hhkerb_resrej_la():
    raw = get_data()
    hhkerb_rej_qtr = raw[(raw.QuestionNumber == 'Q010') & 
                     (raw.ColText == 'Tonnage collected for recycling but actually rejected/disposed')&
                     (raw.Data > 0)]
    hhkerb_rej_la = (hhkerb_rej_qtr.groupby('Authority').agg(np.sum).reset_index()
                     .drop(['Period','QuestionNumber','QuText','RowText',
                            'ColText','MaterialGroup'],axis=1))
 
    #Merge rejected recycling (hhkerb_rej_la) into residual
    hhkerb_res_la = get_hhkerb_res_la()
    merge = hhkerb_res_la.merge(hhkerb_rej_la, how='left', on='Authority')
    merge['Data'] = merge['Data'].replace(np.NaN, 0)
    merge['Collected household waste : Regular Collection'] = merge['Collected household waste : Regular Collection']+ merge['Data']
    merge = merge.drop(['Data'],axis=1)
    return merge

def get_hhkerb_res_drs(reject = 'No', method='WRAP'):
    if reject == 'No':
        hhkerb_res_la = get_hhkerb_res_la()
    if reject == 'Yes':
        hhkerb_res_la = get_hhkerb_resrej_la()
    
    #These are WRAP rates
    glass = 0.0204
    plastics = 0.0151
    ferrous = 0.001554
    alum = 0.003255
    cartons = 0.0037
    if method=='Eunomia':
        glass = 0.0215
        plastics = 0.006
    
    hhkerb_res_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                             'DRS Plastic Bottles','DRS Ferrous Cans',
                                             'DRS Aluminium Cans','DRS Beverage Cartons'])
    hhkerb_res_drs['Authority'] = hhkerb_res_la['Authority']
    #Wrap rate is 0.0204, Eunomia rate is ...
    hhkerb_res_drs['DRS Glass Bottles'] = hhkerb_res_la['Collected household waste : Regular Collection']*glass
    #WRAP rate is 0.0151, Eunomia rate is 0.006048
    hhkerb_res_drs['DRS Plastic Bottles'] = hhkerb_res_la['Collected household waste : Regular Collection']*plastics
    hhkerb_res_drs['DRS Ferrous Cans'] = hhkerb_res_la['Collected household waste : Regular Collection']*ferrous
    hhkerb_res_drs['DRS Aluminium Cans'] = hhkerb_res_la['Collected household waste : Regular Collection']*alum
    hhkerb_res_drs['DRS Beverage Cartons'] = hhkerb_res_la['Collected household waste : Regular Collection']*cartons
    hhkerb_res_drs.to_csv(op.join(data_dir, ('hhkerb_res_drs_'
                                               + dt.datetime.today().strftime("%d%m")
                                               + '.csv')), 
                            encodings = 'utf-8')
    return hhkerb_res_drs

"""
HWRCs Recycling

"""

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
    #Get dry recycling
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
    return merge

def get_hwrcs_recreu_la():
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
    hwrcs_reu_bring_la = (hwrcs_reu_bring.groupby('Authority')['Data']
                          .agg(np.sum).to_frame().reset_index()
                          .rename(columns={'Data':'sum_dry_rec'}))
    #Combine the two dataframes (add up reuse data for each LA)
    merge = hwrcs_reu_ca_la.merge(hwrcs_reu_bring_la, how='outer', on='Authority')
    merge = merge.replace(np.NaN, 0)
    merge['sum_dry_rec'] = merge['sum_dry_rec_x'] + merge['sum_dry_rec_y']
    merge = merge.drop(['sum_dry_rec_x','sum_dry_rec_y'],axis=1)
        
    #Merge reuse data into hwrcs_rec_la from get_hwrcs_rec_la()
    hwrcs_rec_la = get_hwrcs_rec_la()
    merge = hwrcs_rec_la.merge(merge, how='left', on='Authority')
    merge = merge.replace(np.NaN, 0)
    merge['sum_dry_rec'] = merge['sum_dry_rec_x'] + merge['sum_dry_rec_y']
    merge = merge.drop(['sum_dry_rec_x','sum_dry_rec_y'],axis=1)
    merge.to_csv(op.join(data_dir, ('hwrcs_rec_la_'+ dt.datetime.today()
                                    .strftime("%d%m")+ '.csv')),encodings = 'utf-8')
    return merge

def get_hwrcs_rec_drs(reuse = 'No', dry_rec = 'Sum'):
    if reuse == 'No':
        hwrcs_rec_drs = get_hwrcs_rec_la()
    if reuse is 'Yes':
        hwrcs_rec_drs = get_hwrcs_recreu_la()
    
    #Use Eunomia's rates
    #Use WRAP rates from its 2009 MRF Quality Assessment Study for co-mingled materials
    mixed_glass = 0.75
    clear_glass = 0.35
    co_glass = 0.0245
    plastics = 0.50
    co_plastics = 0.16528
    mixed_fer = 0.80 
    mixed_alum = 0.20
    co_fer = 0.1123
    co_alum = 0.0379
    co_bev = 0.00669
    
    if dry_rec == 'Sum':
        dry_rec = 'sum_dry_rec'
    if dry_rec == 'Comingled':
        dry_rec = 'Co mingled materials'
    
    #DRS Glass Bottles (derived from 'Brown glass','Clear glass','Green glass',
    #'Mixed glass', or co-mingled dry recycling materials)
    hwrcs_rec_drs['DRS Glass Bottles'] = (hwrcs_rec_drs['Mixed glass'] * mixed_glass 
                                          + hwrcs_rec_drs['Clear glass'] * clear_glass
                                          + hwrcs_rec_drs['Brown glass'] 
                                          + hwrcs_rec_drs['Green glass'])
    hwrcs_rec_drs['DRS Glass Bottles'] = (hwrcs_rec_drs['DRS Glass Bottles']
                                            .replace(0, 
                                                     hwrcs_rec_drs[dry_rec]*co_glass))
    
    #DRS Plastic Bottles (derived from 'Mixed Plastic Bottles', 'Plastics', or co-mingled)
    #Use data from 'Mixed Plastic Bottles'. If unavailable, use data from 'Plastics'
    #'Mixed Plastic Bottles' are treated as 100% DRS, and Eunomia rate for 'Plastics' is 50%
    hwrcs_rec_drs['DRS Plastic Bottles'] = hwrcs_rec_drs['Mixed Plastic Bottles']
    hwrcs_rec_drs['DRS Plastic Bottles'] = (hwrcs_rec_drs['DRS Plastic Bottles']
                                            .replace(0,hwrcs_rec_drs['Plastics'] * plastics))
    hwrcs_rec_drs['DRS Plastic Bottles'] = (hwrcs_rec_drs['DRS Plastic Bottles']
                                             .replace(0,hwrcs_rec_drs[dry_rec] * co_plastics))
    
    #DRS Ferrous Cans & DRS Aluminium Cans
    #If 'Steel cans' or 'Aluminium cans' exists, use that info
    hwrcs_rec_drs['DRS Ferrous Cans'] = hwrcs_rec_drs['Steel cans']
    hwrcs_rec_drs['DRS Aluminium Cans'] = hwrcs_rec_drs['Aluminium cans']
    #Use 'Mixed cans' if 'Steel cans' or 'Aluminium cans' don't exist
    hwrcs_rec_drs['DRS Ferrous Cans'] = (hwrcs_rec_drs['DRS Ferrous Cans']
                                             .replace(0,hwrcs_rec_drs['Mixed cans'] * mixed_fer))
    #If none of the above exists, use sum of dry recycling, with WRAP MRF rate being ???
    hwrcs_rec_drs['DRS Ferrous Cans'] = (hwrcs_rec_drs['DRS Ferrous Cans']
                                         .replace(0,hwrcs_rec_drs[dry_rec] * co_fer))
    #Use 'Mixed cans' if 'Steel cans' or 'Aluminium cans' don't exist
    hwrcs_rec_drs['DRS Aluminium Cans'] = (hwrcs_rec_drs['DRS Aluminium Cans']
                                           .replace(0,hwrcs_rec_drs['Mixed cans'] * mixed_alum))
    #If none of the above exists, use sum of dry recycling, with WRAP MRF rate being ???    
    hwrcs_rec_drs['DRS Aluminium Cans'] = (hwrcs_rec_drs['DRS Aluminium Cans']
                                            .replace(0,hwrcs_rec_drs[dry_rec] * co_alum))

    #DRS Beverage Cartons
    #For LAs with info on beverage cartons, use the specific data
    hwrcs_rec_drs['DRS Beverage Cartons'] = hwrcs_rec_drs['Composite food and beverage cartons']
    #For the rest, use WRAP MRS rate of ??? on the sum of dry recycling
    hwrcs_rec_drs['DRS Beverage Cartons'] = (hwrcs_rec_drs['DRS Beverage Cartons']
                                               .replace(0, hwrcs_rec_drs[dry_rec] * co_bev))
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

def get_hwrcs_res_la():
    hwrcs_res_qtr = get_hwrcs_res_qtr()
    hwrcs_res_la = hwrcs_res_qtr.groupby('Authority').agg(np.sum).reset_index()
    return hwrcs_res_la

def get_hwrcs_resrej_la():
    raw = get_data()
    #The only data of rejected materials is Tonnage collected for recycling but actually rejected / disposed
    #for Question 16
    hwrcs_rej_ca_qtr = raw[(raw.QuestionNumber == 'Q016') & 
                     (raw.ColText == 'Tonnage collected for recycling but actually rejected / disposed')&
                     (raw.Data > 0)]
    hwrcs_rej_la = (hwrcs_rej_ca_qtr.groupby('Authority').agg(np.sum).reset_index()
                     .drop(['Period','QuestionNumber','QuText','RowText',
                            'ColText','MaterialGroup'],axis=1))
    
    #Merge rejected recycling (hwrcs_rej_la) into residual (hwrcs_res_la)
    hwrcs_res_la = get_hwrcs_res_la()
    merge = hwrcs_res_la.merge(hwrcs_rej_la, how='left', on='Authority')
    merge['Data'] = merge['Data'].replace(np.NaN, 0)
    merge['Civic amenity sites waste : Household'] = (merge['Civic amenity sites waste : Household'] + merge['Data'])
    merge = merge.drop(['Data'],axis=1)
    return merge

def get_hwrcs_res_drs(reject = 'No'):
    if reject == 'No':
        hwrcs_res_la = get_hwrcs_res_la()
    if reject == 'Yes':
        hwrcs_res_la = get_hwrcs_resrej_la()

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
Commerical Recycling

"""
#Since there are no composition rates for comingled materials, the measure "sum of dry recycling" 
#does not come into play. Thus, do not need to consider "Tonnage for reuse" (irrelevant to DRS)

#Note, for some rates, use ZWS's rates for HWRCs (e.g. 75% Mixed glass is DRS, 50% Plastics is DRS)

def get_com_rec_qtr():
    raw = get_data()
    com_rec = raw[(raw.QuestionNumber == 'Q011') &
                  (raw.ColText == 'Tonnage collected for recycling')]
    com_rec_qtr = com_rec.pivot_table(values='Data', index=['Authority','Period'],
                                      columns='RowText', aggfunc = lambda x: x).reset_index()
    return com_rec_qtr

def get_com_rec_la():
    com_rec_la = (get_com_rec_qtr().groupby('Authority').agg(np.sum).reset_index()
                  .drop(['Green garden waste only','Waste food only'],axis=1))
    #Do not need to generate the sum of recycling materials, 
    #nor necessary to include 'Co mingled materials'
    #Note that DRS Beverage Cartons will be generated based on figures from Commercial Residual
    com_rec_la = com_rec_la[['Authority','Mixed glass','Mixed Plastic Bottles',
                             'Plastics','Mixed cans', 'Co mingled materials']]
    return com_rec_la

def get_com_rec_drs_int():
    #Merge in population for interpolation of missing values
    merge = get_pop().merge(get_com_rec_la(), how='left',on='Authority')
    #For each material, calculate material mass per population from available data, and pick median
    #For LAs with missing data, multiply the median rate and LA's population 
    #to get estimated material mass
    drs_list = ['Mixed glass','Mixed Plastic Bottles','Plastics','Mixed cans']
    for drs in drs_list:
        merge['Estimated ' + drs] = merge['Population']*((merge[drs]/merge['Population']).median())
        merge['Combined ' + drs] = merge[drs].replace(np.NaN, merge['Estimated ' +  drs])
    
    #Initialise dataframe com_rec_drs
    com_rec_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                        'DRS Plastic Bottles','DRS Ferrous Cans',
                                        'DRS Aluminium Cans','DRS Beverage Cartons'])
    com_rec_drs['Authority'] = merge['Authority']

    #For DRS Glass Bottles, use 'Combined Mixed glass' (from raw and estimated data)
    #For now, use ZWS rate for HWRC 'Mixed glass': 75%
    com_rec_drs['DRS Glass Bottles'] = merge['Combined Mixed glass']*.75
    
    #For DRS Plastic Bottles, use 'Mixed Plastic Bottles' or 'Plastics', or respectively estimated data
    #'Mixed Plastic Bottles' are treated as 100% DRS, and HWRCs ZWS rate for 'Plastics' is 50%
    com_rec_drs['DRS Plastic Bottles'] = merge['Mixed Plastic Bottles']
    com_rec_drs['DRS Plastic Bottles'] = (com_rec_drs['DRS Plastic Bottles']
                                            .replace(np.NaN, merge['Plastics']*.5))
    com_rec_drs['DRS Plastic Bottles'] = (com_rec_drs['DRS Plastic Bottles']
                                            .replace(np.NaN, merge['Estimated Plastics']*.5))
        
    #For DRS Ferrous Cans & DRS Aluminium Cans, use 'Mixed cans' or its estimated data
    #ZWS rates: 20% is aluminium, 80% is ferrous
    com_rec_drs['DRS Ferrous Cans'] = merge['Combined Mixed cans']*.8
    com_rec_drs['DRS Aluminium Cans'] = merge['Combined Mixed cans']*.2
    
    #For DRS Beverage Cartons, use ZWS estimated recycling rate of 30% and com_res_drs to interpolate
    #First, get com_res_drs
    com_res_drs = get_com_res_drs()
    #For each LA, calculate: DRS Beverage Cartons from com_res_drs multiplied by 30%/70%
    com_rec_drs['DRS Beverage Cartons'] = com_res_drs['DRS Beverage Cartons']*(.3/.7)
    
    com_rec_drs.to_csv(op.join(data_dir, ('com_rec_drs_' + dt.datetime.today().strftime("%d%m")
                                          + '.csv')), encodings = 'utf-8')
    
    return com_rec_drs

def get_com_rec_drs_zws():
    #This is an alternative method to estimate DRS rates (from com_res_drs and recycling rates)
    com_res_drs = get_com_res_drs()
    test_com_rec_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                        'DRS Plastic Bottles','DRS Ferrous Cans',
                                        'DRS Aluminium Cans','DRS Beverage Cartons'])
    test_com_rec_drs['Authority'] = com_res_drs['Authority']
    test_com_rec_drs['DRS Glass Bottles'] = com_res_drs['DRS Glass Bottles']*(.6/.4)
    test_com_rec_drs['DRS Plastic Bottles'] = com_res_drs['DRS Plastic Bottles']*(.3/.7)
    test_com_rec_drs['DRS Ferrous Cans'] = com_res_drs['DRS Ferrous Cans']*(.4/.6)
    test_com_rec_drs['DRS Aluminium Cans'] = com_res_drs['DRS Aluminium Cans']*(.4/.6)
    test_com_rec_drs['DRS Beverage Cartons'] = com_res_drs['DRS Beverage Cartons']*(.3/.7)
    return test_com_rec_drs

"""
Commerical Residual

"""

def get_com_res_la():
    raw = get_data()
    res = (raw[(raw.QuestionNumber == 'Q023') & (raw.ColText == 'Tonnage')]
           .pivot_table(values='Data', index=['Authority','Period'],
                        columns='RowText', aggfunc = lambda x: x).reset_index())
    com_res_qtr = res[['Authority','Period','Collected non-household waste : Commercial & Industrial']]
    com_res_la = com_res_qtr.groupby('Authority').agg(np.sum).reset_index()
    return com_res_la

def get_com_res_drs():
    com_res_la = get_com_res_la()
    #Merge in population for interpolation of missing values
    merge = get_pop().merge(get_com_res_la(), how='left', on='Authority')
    #Calculate material mass per population from available data, and pick median
    #For the three LAs with missing data, multiply the median rate and LA's population 
    #to get estimated material mass
    drs = 'Collected non-household waste : Commercial & Industrial'
    merge['Estimated ' + drs] = merge['Population']*((merge[drs]/merge['Population']).median())
    merge['Combined ' + drs] = merge[drs].replace(np.NaN, merge['Estimated ' +  drs])
    
    #Initialise com_res_drs dataframe
    com_res_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                    'DRS Plastic Bottles','DRS Ferrous Cans',
                                    'DRS Aluminium Cans','DRS Beverage Cartons'])
    com_res_drs['Authority'] = merge['Authority']
    #Aplying ZWS rate: 35% of clear glass is DRS
    com_res_drs['DRS Glass Bottles'] = merge['Combined ' + drs]*0.0216 
    com_res_drs['DRS Plastic Bottles'] = merge['Combined ' + drs]*0.0234
    com_res_drs['DRS Ferrous Cans'] = merge['Combined ' + drs]*0.0068
    com_res_drs['DRS Aluminium Cans'] = merge['Combined ' + drs]*0.0032
    com_res_drs['DRS Beverage Cartons'] = merge['Combined ' + drs]*0.0028
    return com_res_drs

"""
Litter Recycling

"""
#There is no data for 'Tonnage collected for reuse'
#There is minimal data for Street Recycling Bins
#Consider combining both


"""
Litter Residual

"""
#There is no data for 'Tonnage collected for recycling but actually rejected/disposed'
#from Street Recycling Bins (Q034), so the only Litter Residual is from Q023 

#Litter residual is calculated using the following equation:
#Street Cleaning (Q023) - Flytipping - Mechanical Sweeping - Bin Litter

def get_lit_res_la():
    raw = get_data()
    res = (raw[(raw.QuestionNumber == 'Q023') & (raw.ColText == 'Tonnage')]
             .pivot_table(values='Data', index=['Authority','Period'],
                          columns='RowText', aggfunc = lambda x: x).reset_index())
    #Street Cleaning
    lit_str_qtr = res[['Authority','Period','Collected household waste : Street Cleaning']]
    lit_str_la = lit_str_qtr.groupby('Authority').agg(np.sum).reset_index()
    
    #Flytipping
    lit_fly_qtr = res[['Authority','Period','Waste Arising from clearance of fly-tipped materials']]
    lit_fly_la = lit_fly_qtr.groupby('Authority').agg(np.sum).reset_index()

    #Final calculation for litter 
    #(based on the WRAP estimation that 50% of Street Cleaning is Mechanical Sweeping)
    merge = lit_str_la.merge(lit_fly_la, how='left',on='Authority')
    merge = merge.replace(np.NaN, 0)
    merge['Litter'] = ((merge['Collected household waste : Street Cleaning']/2) 
                       - merge['Waste Arising from clearance of fly-tipped materials'])
    return merge

def get_lit_res_drs():
    lit_res_la = get_lit_res_la()
    
    #Initialise com_res_drs dataframe
    lit_res_drs = pd.DataFrame(columns=['Authority','DRS Glass Bottles',
                                    'DRS Plastic Bottles','DRS Ferrous Cans',
                                    'DRS Aluminium Cans','DRS Beverage Cartons'])
    lit_res_drs['Authority'] = lit_res_la['Authority']
    #Aplying ZWS rate: all packaging glass, plastic bottles, metal cans are DRS
    #Plastic bottle rates could be "PET & HDPE", or "PET, HDPE & other bottles"...
    #Which one to use? Currently using just "PET & HDPE"
    lit_res_drs['DRS Glass Bottles'] = lit_res_la['Litter']*0.0688
    lit_res_drs['DRS Plastic Bottles'] = lit_res_la['Litter']*0.0712
    lit_res_drs['DRS Ferrous Cans'] = lit_res_la['Litter']*0.0183
    lit_res_drs['DRS Aluminium Cans'] = lit_res_la['Litter']*0.0369
    lit_res_drs['DRS Beverage Cartons'] = lit_res_la['Litter']*0.0045
    return lit_res_drs

"""
Total weight modelled for each DRS material

"""

def get_total_weight_drs_list(ave_pet_size = 0.75):
    scot_wgt_list = [164.8, 38.6, 5.2, 8.9, 5, 222.5, 0]
    scot_pop = 5347600.0
    wales_pop = 3092000.0
    uk_pop = 64596800.0
    wales_scot_ratio = wales_pop / scot_pop
    wales_uk_ratio = wales_pop / uk_pop
    #(Categories: Glass, PET, HDPE, Ferrous, Aluminium, Carton)
    ave_kg_list = [0.378, 0.033, 0.056, 0.035, 0.017, 0.021] #From Eunomia p.A13 & p.A 
    
    #Glass
    wales_gla_wgt = scot_wgt_list[0] * wales_scot_ratio
    
    #PET
    uk_pet_vol = 14800000000 * 0.69
    wales_pet_vol = uk_pet_vol * wales_uk_ratio
    ave_pet_size = 0.5 #Can change between 0.5 L to 1.5 L
    wales_pet_num = wales_pet_vol / ave_pet_size
    wales_pet_wgt = wales_pet_num * ave_kg_list[1] / 1000000

    #HDPE
    uk_hdpe_num = 4000000000
    wales_hdpe_num = uk_hdpe_num * wales_uk_ratio
    wales_hdpe_wgt = wales_hdpe_num * ave_kg_list[2] / 1000000

    #Plastic
    wales_pla_wgt = wales_pet_wgt + wales_hdpe_wgt

    #Cans
    uk_cans_num = 9800000000
    wales_cans_num = uk_cans_num * wales_uk_ratio
    wales_fer_num = wales_cans_num * 0.22
    wales_alum_num = wales_cans_num * 0.78
    wales_fer_wgt = wales_fer_num * ave_kg_list[3] / 1000000
    wales_alum_wgt = wales_alum_num * ave_kg_list[4] / 1000000

    #Cartons
    uk_car_wgt = 60
    wales_car_wgt = uk_car_wgt * wales_uk_ratio
     
    #Combined list
    wales_total_wgt = wales_gla_wgt + wales_pla_wgt + wales_fer_wgt + wales_alum_wgt + wales_car_wgt
    wales_wgt_list = [wales_gla_wgt, wales_pla_wgt, wales_fer_wgt, wales_alum_wgt, wales_car_wgt, 
                      wales_total_wgt, 0]
    return wales_wgt_list

"""
Mass flow baseline master function

"""
def get_massflow_baseline(reuse = 'No', reject = 'No', hhkerb_rec_method = 'WRAP', 
                          com_rec_method = 'Interpolation', dry_rec_method = 'Sum'):
    """
    Input: The tonnages of DRS materials from all seven streams of mass flow for each local authority.
    The streams are: Household Kerbside Recycling, Household Kerbside Residual, 
    HWRCs Recycling, HWRCs Residual, Commercial Recycling, Commercial Residual, Litter Residual
    
    Output: A dataframe of the mass flow baseline that contains the aggregated tonnages of DRS materials
    for Wales in the year (April 2014 to March 2015). Additional data include: total mass of materials 
    from sold containers, and the calculated remains of DRS materials in the environment
    """
    
    if com_rec_method == 'Interpolation':
        com_rec_drs = get_com_rec_drs_int()
    if com_rec_method == 'Eunomia':
        com_rec_drs = get_com_rec_drs_zws()

    #For each stream, aggregate DRS tonnages from individual LAs to Wales-level 
    hhkerb_rec_sum = (get_hhkerb_rec_drs(reuse=reuse, method=hhkerb_rec_method, dry_rec=dry_rec_method)
                      .sum(axis=0, numeric_only=True).div(1000)
                      .to_frame().reset_index().rename(columns={'RowText': 'DRS Materials',
                                                     0: 'Household Kerbside Recycling'}))
    hhkerb_res_sum = (get_hhkerb_res_drs(reject=reject).sum(axis=0, numeric_only=True)
                      .div(1000).to_frame().reset_index()
                      .rename(columns={'index': 'DRS Materials',0: 'Household Kerbside Residual'}))
    hwrcs_rec_sum = (get_hwrcs_rec_drs(reuse=reuse, dry_rec=dry_rec_method)
                     .sum(axis=0, numeric_only=True).div(1000).to_frame()
                     .reset_index().rename(columns={'RowText': 'DRS Materials',0: 'HWRCs Recycling'}))
    hwrcs_res_sum = (get_hwrcs_res_drs(reject=reject).sum(axis=0, numeric_only=True).div(1000)
                     .to_frame().reset_index()
                     .rename(columns={'index': 'DRS Materials',0: 'HWRCs Residual'}))
    com_rec_sum = (com_rec_drs.sum(axis=0, numeric_only=True).div(1000).to_frame()
                   .reset_index().rename(columns={'index': 'DRS Materials',0: 'Commercial Recycling'}))
    com_res_sum = (get_com_res_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                   .reset_index().rename(columns={'index': 'DRS Materials',0: 'Commercial Residual'}))
    lit_res_sum = (get_lit_res_drs().sum(axis=0, numeric_only=True).div(1000).to_frame()
                   .reset_index().rename(columns={'index': 'DRS Materials',0: 'Litter Residual'}))
    
    #Initialise baseline dataframe
    drs_list = ['DRS Glass Bottles','DRS Plastic Bottles','DRS Ferrous Cans','DRS Aluminium Cans',
                'DRS Beverage Cartons','Total', 'Percent Contribution']
    
    #If using Scotland's number of containers from Eunonmia to estimate Wales data:
    #The tonnages from Scotland were calculated from Eunomia's number of containers and average weight
    #for each DRS material
    #scot_wgt_list = [164.8, 38.6, 5.2, 8.9, 5, 222.5, 0]
    #baseline = pd.DataFrame(data={'DRS Materials':drs_list,
    #                              'Total Weight in Thousand Tonnes':scot_wgt_list})
    #baseline['Total Weight in Thousand Tonnes'] = baseline['Total Weight in Thousand Tonnes']*.5782
    
    #If using data gathered by Joan (sources specified in report), use get_total_weight_drs()
    wales_wgt_list = get_total_weight_drs_list()
    baseline = pd.DataFrame(data={'DRS Materials':drs_list,
                                  'Total Weight in Thousand Tonnes': wales_wgt_list})
    
    #Merge in tonnage from every stream
    baseline = baseline.merge(hhkerb_rec_sum, how = 'left', on='DRS Materials')
    baseline = baseline.merge(hhkerb_res_sum, how = 'left', on='DRS Materials')
    baseline = baseline.merge(hwrcs_rec_sum, how = 'left', on='DRS Materials')
    baseline = baseline.merge(hwrcs_res_sum, how = 'left', on='DRS Materials')
    baseline = baseline.merge(com_rec_sum, how = 'left', on='DRS Materials')
    baseline = baseline.merge(com_res_sum, how = 'left', on='DRS Materials')
    baseline = baseline.merge(lit_res_sum, how = 'left', on='DRS Materials')
    
    #Calculate the rest of the measures
    baseline = baseline.replace(np.NaN, 0)
    #Remains in environment could hopefully be calculated from all other measures, not a 1% estimation
    #baseline['Remains in Environment (1%)'] = baseline['Total Weight in Thousand Tonnes']*.01
    #Calculate the total from each stream
    stream_list = ['Household Kerbside Recycling','Household Kerbside Residual',
                   'HWRCs Recycling','HWRCs Residual',
                   'Commercial Recycling','Commercial Residual',
                   'Litter Residual']
    for stream in stream_list:
        baseline[stream] = np.where(baseline['DRS Materials']=='Total',
                                    sum(baseline[stream]),baseline[stream])
    #Calculate our own estimate of "Remains in Environment"
    baseline['Remains in Environment (leftover)'] = (baseline['Total Weight in Thousand Tonnes'] 
                                              - baseline['Household Kerbside Recycling']
                                              - baseline['Household Kerbside Residual']
                                              - baseline['HWRCs Recycling']
                                              - baseline['HWRCs Residual']
                                              - baseline['Commercial Recycling']
                                              - baseline['Commercial Residual']
                                              - baseline['Litter Residual'])
    #Calculate percent contribution of each stream
    stream_list.append('Remains in Environment (leftover)')
    for stream in stream_list:
        baseline[stream] = np.where(baseline['DRS Materials']=='Percent Contribution',
                                    (baseline[baseline['DRS Materials']=='Total'][[stream]]
                                    .div(1.402216, axis='index')[stream]),
                                    baseline[stream])
    baseline['Total Weight in Thousand Tonnes'] = np.where((baseline['DRS Materials']==
                                                          'Percent Contribution'), 100, 
                                                         baseline['Total Weight in Thousand Tonnes'])
    
    #Export to .csv
    baseline.to_csv(op.join(data_dir, ('massflow_baseline_' 
                                       + reuse + 'reuse_'
                                       + reject + 'reject_'
                                       + hhkerb_rec_method + '_'
                                       + com_rec_method + '_'
                                       + dt.datetime.today().strftime("%d%m")
                                       + '.csv')), 
                    encodings = 'utf-8')
    return baseline

                            

    
                                                     

