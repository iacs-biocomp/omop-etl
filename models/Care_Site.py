from models.Base import Base
from models.models import CareSite
import config
import os
import pandas as pd
import utils.cache.cache as cache
from utils.OmopDB import OmopDB

class Care_Site(Base):
    """ This class define the care_site tables. 
    This table contains a list of uniquely identified institucional (physical or organizational) units 
    where healthcare deliery is practiced (offices, wards, hospitals, clinics) """
    def __init__(self, caches):
        self.model=CareSite
        self.domain='CareSite'
        self.caches=caches

    def mapData(self, df):

        lastPK=self.getLastPK()
        df['care_site_id']=range(lastPK+1,lastPK+1+len(df))
        

        df['care_site_source_value'] = df['location']

        df=self.mapLocation(df)

        return super().mapData(df)

    def cleanData(self, df, module_name):
        return super().cleanData(df, module_name)

    def insertData(self, df):
        return super().insertData(df)

    def process(self, df):
        return super().process(df)


    def mapLocation(self, df):
        """ This method bring location_id from location table to care_site table """

        if 'location' not in df.columns:
            return df
        
        if 'location' not in self.caches:
            import utils.cache.cache as cache
            
            self.caches.set('location',cache.load_cache('location',self.getOmopConnection()))
            


        df['location_id']=df.location.map(self.caches['location'].get)
        df['location_id']=df['location_id'].astype('Int64')   
        return df