
import pandas as pd
import numpy as np




def cleanfunc(kildesæt, name):
    '''Cleaning function that takes a source dataset or list of datasets, and a dataset name or list of dataset names as arguments'''
    kildesæt = pd.read_csv(kildesæt)
    kildesæt = kildesæt.drop(columns=['event_type', 'book_id', 'image_id', 'event_month', 'event_day', 'event_year', 'household_id', 'image_appearance'])
    kildesæt = kildesæt.rename(columns={"marital_status":"marital_status_" + name , "age":"age_" + name, "event_location":"event_location_" 
                                        + name, 'event_town':'event_town_' + name, "event_parish":"event_parish_" + name, "event_county":"event_county_" + name, 
                                        'event_district':'event_district_' + name, 
                                        "event_country":"event_country_" + name, "household_position":"household_position_" + name, "role":"role_" 
                                        + name, 'event_date':'event_date_' + name})
    kildesæt['pa_id'] = kildesæt['pa_id'].astype(str)
    return kildesæt



def mergefunc(initialsæt, kildesæt, kildenummer):
    '''Merging function that aggregates the information from a single source dataset or list of source datasets into a useable format where observations are uniquely 
    identified across sources. Arguments: initialsæt; the life-courses dataset that uniquely identifies each observation. Kildesæt; A source dataset, e.g. a census. 
    Kildenummer; the enumerated source number according to the link-lives project. See also: Readme.me.'''
    
    
    num_sources = max([len(str(pa_ids).split(',')) for pa_ids in initialsæt['pa_ids']])  #identifies the number of sources
    
    #merge the information from the source to the correct observations in the life-courses dataset:
    outputsæt = initialsæt[initialsæt[f'source{0}'] == kildenummer].merge(kildesæt, left_on=f'pa_id{0}', right_on='pa_id', how='left') 

    for i in range(1, num_sources): #iterates over the number of sources 
        joinprep = initialsæt[initialsæt[f'source{i}'] == kildenummer] #identifies observations that are in a given source number
        join = pd.merge(joinprep, kildesæt, left_on=f'pa_id{i}', right_on='pa_id', how='left') #merges the observations with the correct pa_id in the correct source
        outputsæt = pd.concat([outputsæt, join]) #concatenates the information to the output dataframe
        
    return outputsæt