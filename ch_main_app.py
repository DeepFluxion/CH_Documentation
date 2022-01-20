#!/usr/bin/env python
# coding: utf-8
# ---
# jupyter:
#   hide_input: false
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython3
#     version: 3.8.3
#   nbTranslate:
#     displayLangs:
#     - '*'
#     hotkey: alt-t
#     langInMainMenu: true
#     sourceLang: en
#     targetLang: fr
#     useGoogleTranslate: true
#   nbrmd_format_version: '1.1'
# ---


import datetime
# this variable is just to controle the process
initial_time = datetime.datetime.now()


# this variable is just to controle the process
t2 = datetime.datetime.now()

# import the other packages
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import os
import pandas as pd

import requests

import requests
from lxml import html

import gspread
from google.oauth2 import service_account
# this variable is just to controle the process
t3 = datetime.datetime.now()


# # Global Variables


#size of space of search 
n = 30


# Companies House apiKey
apiKey = 'c2296c08-722b-44bd-bc74-9384d70284ad'
# Variables for usig gspread and google-auth
scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
json_file = "credentials.json"
spreadsheet_name =  'Companies_House_Prospects'
spreadsheet_page =  'prospects_list'
# this variable is just to controle the process
t4 = datetime.datetime.now()


# The name of download folder
down_label = 'downloads2'


# this is a list of sic numbers to be searched
sic_numbers_all = ['56101','56102','56103']


# # Search and collect data functions

# for functions see the docstring bellow
def search_files(down_label = 'downloads2', search_dates = [datetime.date.today().strftime('%d/%m/%Y')]):
    '''
    The search_files() function open a research the url advanced search of Companies House and 
    download a csv file for each sic number and for each date in the search_dates. The files ar saved in 
    down_path directory.
    Parameters:
    * down_label - String. The parameter down_label is the name of the directory where the files will be saved
    * search_dates - list. The parameter search_dates is a list with all dates to search, the default is today date
    Returns
    * The search returns results it is printed the date, the sic and ok, 
    '''
    

    download_log =[]
    down_path = os.path.join(os.getcwd(),down_label)
    # Another Selenium configurations variables
    options = webdriver.ChromeOptions()
    prefs = {'download.default_directory' : down_path}
    options.add_experimental_option('prefs', prefs)

    for search_date in search_dates:
        #
        i=0
        for sic_number in sic_numbers_all:
            browser = webdriver.Chrome(options=options)
            browser.get("https://find-and-update.company-information.service.gov.uk/advanced-search") 
            time.sleep(2)
            colapse_button = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div/div/form/div[1]/div[1]/button')
            colapse_button.click()
            initial_date = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div/div/form/div[1]/div[4]/div[2]/fieldset/div[1]/input')
            initial_date.send_keys(search_date)
            final_date = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div/div/form/div[1]/div[4]/div[2]/fieldset/div[2]/input')
            final_date.send_keys(search_date)
            active_check = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div/div/form/div[1]/div[5]/div[2]/fieldset/div/div/fieldset/div/div[1]')
            active_check.click()
            open_check = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div/div/form/div[1]/div[5]/div[2]/fieldset/div/div/fieldset/div/div[3]')
            open_check.click()
            nature = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div/div/form/div[1]/div[6]/div[2]/div/input')
            nature.send_keys(sic_number)
            search_button = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div/div/form/div[2]/button[1]')
            search_button.click()
            try:
                down_button = browser.find_element_by_xpath('/html/body/div[3]/div[1]/main/div/div[2]/div[2]/div[1]/form/div[2]/button')
                down_button.click()
                time.sleep(3)
                strings = [str(i), str(search_date),str(sic_number), 'OK']
                download_log.append(' '.join(strings))
                #print(' '.join(strings))
            except:
                strings = [str(i), str(search_date),str(sic_number), 'Sem resultados para busca']
                download_log.append(' '.join(strings))
                #print(' '.join(strings))
            browser.close()
        i+=1
        return


# for functions see the docstring bellow
def create_prospects(down_label = 'downloads2'):
    #
    '''
    The create_prospects function generates a pandas dataframe for all files saved in the down_path folder.
    Parameters:
    * down_label - String. The parameter down_label is the name of the directory where the files will be saved
    Returns
    * prospects a a pandas dataframe, 
    '''
    down_path = os.path.join(os.getcwd(),down_label)
    files = os.listdir(down_path)
    dff = []
    for file in files:
        df=pd.read_csv(os.path.join(down_path,file))
        dff.append(df)
        prospects = pd.concat(dff, axis=0, ignore_index=True)
        prospects=prospects.drop_duplicates().sort_values(by='incorporation_date')
        prospects.company_number=prospects.company_number.astype('str')
        prospects.nature_of_business = prospects.nature_of_business.str.replace(' ',' | ')
    return prospects


# for functions see the docstring bellow
def collect_officers(prospects):
    '''
    The collect_officers function executes a request for each company_number of the prospects dataframe and returns the 
    names of the officers of this company. If there is no information available about the officers, 
    the following message is returned in the respective row 
    'There are no officer details available for this company.'
    Parameters:
    * prospects a a pandas dataframe, created with create_prospects function
    Returns:
    * all_company_officers_list a list of all officers founded for each company_names 
    * all_company_numbers_sucessed a list of all comapany_numbers which were founed the officers 
    * urls_errors a list of all urls whose requestes returned an error
    '''
    #
    register = prospects.to_dict('records')
    all_company_officers_list = []
    all_company_numbers_sucessed = []
    urls_errors = []
    j=0
    for row in register:
        q = row['company_number']
        url = "https://api.company-information.service.gov.uk/company/"+q+"/officers"
        try:
            data = requests.get(url, auth=(apiKey, '')).json()
            company_officers_list = []
            if len(data)>0:
                for i in range(len(data['items'])):
                    company_officers_list.append(data['items'][i]['name'])

                string =  ' | '.join(company_officers_list)   
            else:
                string = 'There are no officer details available for this company.'    
            all_company_officers_list.append(string)
            all_company_numbers_sucessed.append(q)
            #print(str(j)+' '+string)
            j+=1
        except:
            print('sleeping...')
            urls_errors.append(url)
            continue
    return all_company_officers_list, all_company_numbers_sucessed, urls_errors


# for functions see the docstring bellow
def colect_officers_errors(urls_errors):
    '''
    The create_prospects_erros function executes a request for each company_number in the urls_errors list 
    and updates the names of this company's officers in the all_company_officers_list, all_company_numbers_sucessed, 
    urls_errors lists. 
    If there is no information available about the officers, the message is returned
    Parameters:
    * prospects a a pandas dataframe, created with create_prospects function
    Returns:
    * all_company_officers_list a list of all officers founded for each company_names 
    * all_company_numbers_sucessed a list of all comapany_numbers which were founed the officers 
    * urls_errors a list of all urls whose requestes returned an error
    '''
    
    j=0
    for url in urls_errors:
        
        try:
            data = requests.get(url, auth=(apiKey, '')).json()
            company_officers_list = []
            if len(data)>0:
                for i in range(len(data['items'])):
                    company_officers_list.append(data['items'][i]['name'])

                string =  ' | '.join(company_officers_list)   
            else:
                string = 'There are no officer details available for this company.'    
            all_company_officers_list.append(string)
            all_company_numbers_sucessed.append(q)
            #print(str(j)+' '+string)
            j+=1
        except:
            print('sleeping...')
            urls_errors.append(url)
            continue
    return all_company_officers_list, all_company_numbers_sucessed, urls_errors


# # Google Sheets API Functions

# for functions see the docstring bellow
def login(json_file=json_file):
    '''
    Create a connection instance of Google Sheets API, the jason file needs to be save in the app directory
    Parameters:
    * json_file = the credentials file generated when enable Google Sheets API.
    Returns:
    * gc - a connection instance of Google Sheets API
    '''
    credentials = service_account.Credentials.from_service_account_file(json_file)
    scoped_credentials = credentials.with_scopes(scopes)
    gc = gspread.authorize(scoped_credentials)
    return gc


# for functions see the docstring bellow
def spread_reader(spreadsheet_name, spreadsheet_page):
    '''
    Read a spreadsheet page in a spreadsheet_name
    Parameters:
    * spreadsheet_name - string : The spreadsheet name
    * spreadsheet_page - string : The spreadsheet page name
    Returns:
    * df - a pandas DataFrame
    '''
    gc = login()
    spread = gc.open(spreadsheet_name)
    spread_page = spread.worksheet(spreadsheet_page)
    all_data = spread_page.get_all_values()
    df = pd.DataFrame(all_data)
    header_row = 0
    df.columns = df.iloc[header_row]
    df = df.drop(header_row)
    df.reset_index(drop=True, inplace=True)
    return df


# for functions see the docstring bellow
def spread_writer(list1,spreadsheet_name, spreadsheet_page):
    '''
    write in the last row of a spreadsheet a the values of a list
    Parameters:
    * list - a list
    * spreadsheet_name - string : The spreadsheet name
    * spreadsheet_page - string : The spreadsheet page name
    Returns:
    * The spreadsheet updated
    '''
    gc = login()
    spread = gc.open(spreadsheet_name)
    spread = spread.worksheet(spreadsheet_page)
    spread.append_row(list1, value_input_option='USER_ENTERED')


# this code generate a list of the last n days to be researched search space

base = datetime.datetime.today()
search_dates = [base - datetime.timedelta(days=x) for x in range(n)]
for z,d in enumerate(search_dates):
    search_dates[z]=d.strftime('%d/%m/%Y')


# this code remove all files of down_path
down_path = os.path.join(os.getcwd(),down_label)
filelist = [ f for f in os.listdir(down_path) ]
if len(filelist)>0:
    for f in filelist:
        os.remove(os.path.join(down_path, f))
# this variable is just to controle the process
t5 = datetime.datetime.now()


# here we download the returned csv files from the site
for search_date in search_dates:
    search_files(down_label = 'downloads2', search_dates = [search_date])
# this variable is just to controle the process
t6= datetime.datetime.now()


# here we create the df prospects

prospects = create_prospects(down_label = 'downloads2')
# this variable is just to controle the process
t7 = datetime.datetime.now()


# this code collect the officers names for each prospect
all_company_officers_list, all_company_numbers_sucessed, urls_errors=collect_officers(prospects=prospects)
# this variable is just to controle the process
t8 = datetime.datetime.now()


# Data Prep
officers_suceed = pd.DataFrame(zip(all_company_officers_list,all_company_numbers_sucessed))
officers_suceed.columns=['company_officers_names', 'company_numbers_iden']
prospects = prospects.drop_duplicates().merge(officers_suceed.drop_duplicates(), left_on='company_number',right_on='company_numbers_iden', how='inner')


# Data Prep
prospects.drop(['company_numbers_iden'],axis=1, inplace=True)
prospects.drop(['dissolution_date'],axis=1, inplace=True)
prospects.drop(['company_status'],axis=1, inplace=True)
prospects.fillna('no data for this field', inplace=True)
prospects=prospects.astype('str')


# This code read the Google Sheet file
df = spread_reader(spreadsheet_name, spreadsheet_page)
# this variable is just to controle the process
t9 = datetime.datetime.now()


# Check for new propects
new_prospects = prospects.loc[~prospects.company_number.isin(df.company_number.values.tolist()),:]


# Check are new propects this code will write in the Google Sheet file
if (len(new_prospects)>0):
    #
    for i in new_prospects.index.values.tolist():
        list1 = new_prospects.loc[i,:].values.tolist()
        spread_writer(list1=list1,spreadsheet_name='Companies_House_Prospects', spreadsheet_page='prospects_list')
# this variable is just to controle the process
t10 = datetime.datetime.now()


# the list2 is a list for log control
list2 = [datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
         datetime.datetime.now().strftime('%d/%m/%Y'),
         datetime.datetime.now().strftime('%H:%M:%S'),
         len(new_prospects),
         ' '.join(['Added',str(len(new_prospects)),'new prospects in this log']),
        'Automatic',
        len(urls_errors),
        len(prospects),
        n]



# writing list2 in the log control sheet
spread_writer(list1=list2,spreadsheet_name='Companies_House_Log', spreadsheet_page='Log_Report')
# this variable is just to control the process
t11 = datetime.datetime.now()

# this variable is just to control the process
final_time = datetime.datetime.now()

# the list3is a list for process minning 
list3 = [datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
         (final_time - initial_time).total_seconds(), 
         (t2 - initial_time).total_seconds(), 
         (t3 -t2).total_seconds(),
         (t4-t3).total_seconds(),
         (t5-t4).total_seconds(),
         (t6-t5).total_seconds(),
         (t7-t6).total_seconds(),
         (t8-t7).total_seconds(),
         (t9-t8).total_seconds(),
         (t10-t9).total_seconds(),
         (t11-t10).total_seconds(),
         len(urls_errors),
         len(prospects),
         len(new_prospects),
         n]


# writing list3 in the process minning sheet
spread_writer(list1=list3,spreadsheet_name='Companies_House_Process_minning', spreadsheet_page='process_minning')

print('Finished')
