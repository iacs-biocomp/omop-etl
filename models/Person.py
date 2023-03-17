from models.Base import Base
from models.models import Person as PERSON
import config
import os
import pandas as pd
import utils.cache.cache as cache
from utils.OmopDB import OmopDB

class Person(Base):
    """
    This class transform and insert data that uniquely identify each person or patient, and some demographic information 
     
    """
    def __init__(self, caches):
        self.model=PERSON
        self.domain='Person'
        self.caches=caches

    def mapData(self, df):

        db=OmopDB()
        conn = db.getSession()


        location=cache.load_cache('location', conn, 'omop')
        care_site=cache.load_cache('care_site', conn, 'omop')
        provider = cache.load_cache('provider', conn, 'omop')

        gender={1:8507,2:8532}
        df['person_source_value'] = df.usuario_id
            
        df['gender_concept_id']=df['sexo'].map(gender.get)
        df['gender_concept_id']=df['gender_concept_id'].fillna(0)
        df['birth']=pd.to_datetime(df.nacimiento_dt,errors='coerce', infer_datetime_format=True)
        df=self.reviewErrors(df, 'birth', self.__class__.__module__, dropErrors=True)

        df['birth_datetime']=df['birth']
        df['year_of_birth'] = df['birth'].dt.year
        df['month_of_birth'] = df['birth'].dt.month
        df['day_of_birth'] =df['birth'].dt.day
        df['race_concept_id'] = 0
        df['ethnicity_concept_id'] = 0
        
       

        df['care_site_id']=df['care_site'].map(care_site.get)
        df['care_site_id']=df['care_site_id'].astype('Int64')
        df['location_id']=df['location'].map(location.get)
        df['location_id']=df['location_id'].astype('Int64')
        df['provider_id']=df['provider'].map(provider.get)
        df['provider_id']=df['provider_id'].astype('Int64')
        
        
        df['gender_source_value'] = df['sexo']
        df['gender_source_concept_id'] = 0
        df['race_source_concept_id'] = 0  
        df['ethnicity_source_value'] = 0  


        lastPK=self.getLastPK()
        df['person_id']=range(lastPK+1,lastPK+1+len(df))

        return df

    def cleanData(self, df, module_name):
        return df

    def insertData(self, df):
        return super().insertData(df)

    def process(self, df):
        return super().process(df)


    def getCareSiteMap(self):
        """ This method gets the care_site_id and insert the right care_site_id for the patient in the Person table """
        
        if 'careSiteMap' not in self.caches:
            cdf=pd.read_sql_table('person', self.getOmopConnection(), schema='omop',columns=['person_id', 'care_site_id'])
            self.caches.set('careSiteMap', pd.Series(cdf['care_site_id'].values,index=cdf['person_id'].astype(str)).to_dict())
        return self.caches['careSiteMap']

    def getProviderMap(self):
        """ This method gets the provider_id and insert the right care_site_id for the patient in the Person table """

        if 'providerMap' not in self.caches:
            cdf=pd.read_sql_table('person', self.getOmopConnection(), schema='omop',columns=['person_id', 'provider_id'])
            self.caches.set('providerMap', pd.Series(cdf['provider_id'].values,index=cdf['person_id'].astype(str)).to_dict())
        return self.caches['providerMap']
